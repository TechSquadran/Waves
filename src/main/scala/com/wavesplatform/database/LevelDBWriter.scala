package com.wavesplatform.database

import com.wavesplatform.crypto
import com.wavesplatform.state2._
import com.wavesplatform.state2.reader.LeaseDetails
import org.iq80.leveldb.{DB, ReadOptions}
import scalikejdbc.using
import scodec._
import scodec.bits._
import scodec.codecs._
import scorex.account.{Address, Alias}
import scorex.block.Block
import scorex.transaction.assets.exchange.ExchangeTransaction
import scorex.transaction.assets.{IssueTransaction, ReissueTransaction}
import scorex.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import scorex.transaction.{CreateAliasTransaction, Transaction, TransactionParser}

import scala.collection.mutable.ArrayBuffer

/** The following namespaces are used:
  *
  * address -> waves balance history[]
  * (H, address) -> waves balance
  * address -> lease balance history[]
  * (H, address) -> lease balance
  * address -> asset ids[]
  * (address, asset id) -> asset balance history[]
  * (H, address, asset ID) -> asset balance
  * tx id -> (height, tx bytes)
  * H -> changed addresses[]
  * H -> (address, asset id)[]
  * H -> block
  * H -> txs[]
  *
  */
object LevelDBWriter {


  trait Key[V] {
    def keyBytes: Array[Byte]
    def parse(bytes: Array[Byte]): V
    def encode(v: V): Array[Byte]
  }

  object Key {
    def apply[V](key: Array[Byte], codec: Codec[V]): Key[V] = new Key[V] {
      override def keyBytes = key
      override def parse(bytes: Array[Byte]) = codec.decode(ByteVector(bytes).bits).require.value
      override def encode(v: V) = codec.encode(v).require.toByteArray
    }
  }

  object k {
    private def key[A, V](encoder: Encoder[A], value: A, codec: Codec[V]): Key[V] =
      Key(encoder.encode(value).require.toByteArray, codec)
    private def p[A](c: Codec[A]) = uint16 ~~ c

    private val block = bytes.xmap[Block](bv => Block.parseBytes(bv.toArray).get, b => ByteVector(b.bytes()))
    private val bigInt = bytes.xmap[BigInt](bv => BigInt(bv.toArray), bi => ByteVector(bi.toByteArray))
    private def byteStr(size: Int) = bytes(size).xmap[ByteStr](bv => ByteStr(bv.toArray), bs => ByteVector(bs.arr))
    private val unboundedByteStr = bytes.xmap[ByteStr](bv => ByteStr(bv.toArray), bs => ByteVector(bs.arr))
    private val digestByteStr = byteStr(crypto.DigestSize)
    private val signatureByteStr = byteStr(crypto.SignatureLength)
    private val addressCodec = bytes(Address.AddressLength).exmap[Address](
      bv => Attempt.fromEither(Address.fromBytes(bv.toArray).left.map(e => Err(e.reason))),
      a => Attempt.successful(ByteVector(a.bytes.arr)))

    private val h = p(int32)
    private val address = p(addressCodec)
    private val digest = p(digestByteStr)
    private val signature = p(signatureByteStr)
    private val addressWithHeight = h ~~ addressCodec
    private val digestWithHeight = h ~~ digestByteStr

    private val assetBalanceHistory = address ~~ digestByteStr
    private val assetBalance = addressWithHeight ~~ digestByteStr

    val height: Key[Int] = Key[Int](Array(0, 1), int32)

    def score(height: Int): Key[BigInt] = key(h, (2, height), bigInt)

    def blockAt(height: Int): Key[Block] = key(h, (3, height), block)
    def heightBySignature(blockId: ByteStr): Key[Int] = key(signature, (4, blockId), int32)

    def wavesBalanceHistory(a: Address): Key[List[Int]] = key(address, (5, a), list(int32))

    def wavesBalance(height: Int, address: Address): Key[Long] = key(addressWithHeight, (6, height, address), int64)

    def assetList(a: Address): Key[Set[ByteStr]] = key(address, (7, a), list(digestByteStr).xmap[Set[ByteStr]](_.toSet, _.toList))
    def assetBalanceHistory(a: Address, id: ByteStr): Key[List[Int]] = key(assetBalanceHistory, (8, a, id), list(int32))
    def assetBalance(height: Int, a: Address, id: ByteStr): Key[Long] = key(assetBalance, (9, height, a, id), int64)

    private val assetInfo = (bool(8) :: bigInt).as[AssetInfo]

    def assetInfoHistory(assetId: ByteStr): Key[List[Int]] = key(digest, (10, assetId), list(int32))
    def assetInfo(height: Int, assetId: ByteStr): Key[AssetInfo] = key(digestWithHeight, (11, height, assetId), assetInfo)

    private val leaseBalance = (int64 :: int64).as[LeaseBalance]

    def leaseBalanceHistory(a: Address): Key[List[Int]] = key(address, (12, a), list(int32))
    def leaseBalance(height: Int, address: Address): Key[LeaseBalance] = key(addressWithHeight, (13, height, address), leaseBalance)

    def leaseStatusHistory(leaseId: ByteStr): Key[List[Int]] = key(digest, (14, leaseId), list(int32))
    def leaseStatus(height: Int, leaseId: ByteStr): Key[Boolean] = key(digestWithHeight, (15, height, leaseId), bool(8))

    private val volumeAndFee = (int64 :: int64).as[VolumeAndFee]

    def filledVolumeAndFeeHistory(orderId: ByteStr): Key[List[Int]] = key(digest, (16, orderId), list(int32))
    def filledVolumeAndFee(height: Int, orderId: ByteStr): Key[VolumeAndFee] = key(digestWithHeight, (17, height, orderId), volumeAndFee)


    private val txGuard = Codec[Boolean](
      (_: Boolean) => Attempt.successful(BitVector.empty),
      (bv: BitVector) => Attempt.successful(DecodeResult(bv.nonEmpty, bv)))
    private val transaction = bytes.exmap[Transaction](
      bv => TransactionParser.parseBytes(bv.toArray).fold(e => Attempt.failure(Err(e.getMessage)), t => Attempt.successful(t)),
      t => Attempt.successful(ByteVector(t.bytes()))
    )

    private val txCodec = int32 ~~ optional(txGuard, transaction)

    def transactionInfo(txId: ByteStr): Key[(Int, Option[Transaction])] = key(p(unboundedByteStr), (18, txId), txCodec)

    def addressTransactionHistory(a: Address): Key[List[Int]] = key(address, (19, a), list(int32))
    def addressTransactionIds(height: Int, address: Address): Key[Seq[ByteStr]] = ???


    def changedAddresses(height: Int): Key[List[Address]] = key(h, (20, height), list(addressCodec))
    def transactionIdsAtHeight(height: Int): Key[List[ByteStr]] = key(h, (21, height), list(digestByteStr))

    def addressOfAlias(alias: Alias): Key[Address] = key(int32 ~~ bytes, (22, ByteVector(alias.bytes.arr)), addressCodec)
  }

  class ReadOnlyDB(db: DB, readOptions: ReadOptions) {
    def get[V](key: Key[V]): V = key.parse(db.get(key.keyBytes))
    def opt[V](key: Key[V]): Option[V] = Option(db.get(key.keyBytes)).map(key.parse)
  }

  class RW(db: DB) extends AutoCloseable {
    private val batch = db.createWriteBatch()
    private val snapshot = db.getSnapshot
    private val readOptions = new ReadOptions().snapshot(snapshot)

    def get[V](key: Key[V]): V = key.parse(db.get(key.keyBytes, readOptions))
    def opt[V](key: Key[V]): Option[V] = Option(db.get(key.keyBytes, readOptions)).map(key.parse)

    def put[V](key: Key[V], value: V): Unit = batch.put(key.keyBytes, key.encode(value))

    def delete(key: Array[Byte]): Unit = batch.delete(key)

    def delete[V](key: Key[V]): Unit = batch.delete(key.keyBytes)

    override def close(): Unit = {
      try { db.write(batch) }
      finally {
        batch.close()
        snapshot.close()
      }
    }
  }

  private def loadAssetInfo(db: ReadOnlyDB, assetId: ByteStr) = {
    db.get(k.assetInfoHistory(assetId)).headOption.map(h => db.get(k.assetInfo(h, assetId)))
  }
}

class LevelDBWriter(writableDB: DB) extends Caches {
  import LevelDBWriter._

  private def readOnly[A](f: ReadOnlyDB => A): A = using(writableDB.getSnapshot) { s =>
    f(new ReadOnlyDB(writableDB, new ReadOptions().snapshot(s)))
  }

  override def accountScript(address: Address) = None

  private def readWrite[A](f: RW => A): A = using(new RW(writableDB)) { rw => f(rw) }

  override protected def loadAddressId(address: Address): BigInt = ???

  private def loadHeightOpt(db: ReadOnlyDB) = db.opt(k.height).getOrElse(0)

  override protected def loadHeight(): Int = readOnly(loadHeightOpt)

  override protected def loadScore(): BigInt = readOnly { db => db.opt(k.score(loadHeightOpt(db))).getOrElse(BigInt(0)) }

  override protected def loadLastBlock(): Option[Block] = readOnly { db => db.opt(k.blockAt(loadHeightOpt(db))) }

  override protected def loadWavesBalance(address: Address): Long = readOnly { db =>
    db.opt(k.wavesBalanceHistory(address)).flatMap(_.headOption).fold(0L)(h => db.get(k.wavesBalance(h, address)))
  }

  override protected def loadLeaseBalance(address: Address): LeaseBalance = readOnly { db =>
    db.opt(k.leaseBalanceHistory(address)).flatMap(_.headOption).fold(LeaseBalance.empty)(h => db.get(k.leaseBalance(h, address)))
  }

  override protected def loadAssetBalance(address: Address): Map[ByteStr, Long] = readOnly { db =>
    (for {
      assetId <- db.opt(k.assetList(address)).getOrElse(Set.empty)
      h <- db.opt(k.assetBalanceHistory(address, assetId))flatMap(_.headOption)
    } yield assetId -> db.get(k.assetBalance(h, address, assetId))).toMap
  }

  override protected def loadAssetInfo(assetId: ByteStr): Option[AssetInfo] = readOnly(LevelDBWriter.loadAssetInfo(_, assetId))

  override protected def loadAssetDescription(assetId: ByteStr): Option[AssetDescription] = readOnly { db =>
    db.opt(k.transactionInfo(assetId)) match {
      case Some((_, Some(i: IssueTransaction))) =>
        val reissuable = LevelDBWriter.loadAssetInfo(db, assetId).map(_.isReissuable)
        Some(AssetDescription(i.sender, i.name, i.decimals, reissuable.getOrElse(i.reissuable)))
      case _ => None
    }
  }

  override protected def loadVolumeAndFee(orderId: ByteStr): VolumeAndFee = readOnly { db =>
    db.get(k.filledVolumeAndFeeHistory(orderId)).headOption.fold(VolumeAndFee.empty)(h => db.get(k.filledVolumeAndFee(h, orderId)))
  }

  private def loadLeaseStatus(leaseId: ByteStr) = readOnly { db =>
    db.get(k.leaseStatusHistory(leaseId)).headOption.fold(false)(h => db.get(k.leaseStatus(h, leaseId)))
  }

  private def updateHistory(rw: RW, key: Key[List[Int]], threshold: Int, kf: Int => Key[_]): Seq[Array[Byte]] = {
    val (c1, c2) = rw.opt(key).getOrElse(List.empty).partition(_ > threshold)
    rw.put(key, (height +: c1) ++ c2.headOption)
    c2.drop(1).map(kf(_).keyBytes)
  }

  override protected def doAppend(block: Block,
                                  wavesBalances: Map[Address, Long],
                                  assetBalances: Map[Address, Map[ByteStr, Long]],
                                  leaseBalances: Map[Address, LeaseBalance],
                                  leaseStates: Map[ByteStr, Boolean],
                                  transactions: Map[ByteStr, (Transaction, Set[Address])],
                                  reissuedAssets: Map[ByteStr, AssetInfo],
                                  filledQuantity: Map[ByteStr, VolumeAndFee]): Unit = readWrite { rw =>
    val expiredKeys = new ArrayBuffer[Array[Byte]]

    rw.put(k.height, height)
    rw.put(k.blockAt(height), block)

    val threshold = height - 2000

    for ((address, balance) <- wavesBalances) {
      rw.put(k.wavesBalance(height, address), balance)
      expiredKeys ++= updateHistory(rw, k.wavesBalanceHistory(address), threshold, h => k.wavesBalance(h, address))
    }

    for ((address, leaseBalance) <- leaseBalances) {
      rw.put(k.leaseBalance(height, address), leaseBalance)
      expiredKeys ++= updateHistory(rw, k.leaseBalanceHistory(address), threshold, k.leaseBalance(_, address))
    }

    for ((orderId, volumeAndFee) <- filledQuantity) {
      val kk = k.filledVolumeAndFee(height, orderId)
      rw.put(kk, volumeAndFee)
      expiredKeys ++= updateHistory(rw, k.filledVolumeAndFeeHistory(orderId), threshold, k.filledVolumeAndFee(_, orderId))
    }

    val changedAssetBalances = Set.newBuilder[(Address, ByteStr)]
    for ((address, assets) <- assetBalances) {
      rw.put(k.assetList(address), rw.get(k.assetList(address)) ++ assets.keySet)
      for ((assetId, balance) <- assets) {
        changedAssetBalances += address -> assetId
        rw.put(k.assetBalance(height, address, assetId), balance)
        expiredKeys ++= updateHistory(rw, k.assetBalanceHistory(address, assetId), threshold, k.assetBalance(_, address, assetId))
      }
    }

    for ((leaseId, state) <- leaseStates) {
      rw.put(k.leaseStatus(height, leaseId), state)
      expiredKeys ++= updateHistory(rw, k.leaseStatusHistory(leaseId), threshold, k.leaseStatus(_, leaseId))
    }

    for ((id, (tx, _)) <- transactions) {
      val txToSave = tx match {
        case (_: IssueTransaction | _: LeaseTransaction) => Some(tx)
        case _ => None
      }

      tx match {
        case ca: CreateAliasTransaction => rw.put(k.addressOfAlias(ca.alias), ca.sender.toAddress)
        case _ =>
      }

      rw.put(k.transactionInfo(id), (height, txToSave))
    }

    rw.put(k.transactionIdsAtHeight(height), transactions.keys.toList)
    expiredKeys.foreach(rw.delete)
  }

  override protected def doRollback(targetBlockId: ByteStr): Seq[Block] = readWrite { rw =>
    val value = rw.opt(k.heightBySignature(targetBlockId)).fold(Seq.empty[Int])(_ to height)
    value.flatMap { h =>
      val blockAtHeight = rw.opt(k.blockAt(h))
      for (address <- rw.get(k.changedAddresses(h))) {
        for (assetId <- rw.get(k.assetList(address))) {
          rw.delete(k.assetBalance(h, address, assetId))
          val historyKey = k.assetBalanceHistory(address, assetId)
          rw.put(historyKey, rw.get(historyKey).filterNot(_ == h))
        }

        rw.delete(k.wavesBalance(h, address))
        val wbh = k.wavesBalanceHistory(address)
        rw.put(wbh, rw.get(wbh).filterNot(_ == h))

        rw.delete(k.leaseBalance(h, address))
        val lbh = k.leaseBalanceHistory(address)
        rw.put(lbh, rw.get(lbh).filterNot(_ == h))
      }

      for (txId <- rw.get(k.transactionIdsAtHeight(h))) {
        rw.opt(k.transactionInfo(txId)) match {
          case Some((_, Some(i: IssueTransaction))) =>
          case Some((_, Some(r: ReissueTransaction))) =>
          case Some((_, Some(c: LeaseCancelTransaction))) =>
          case Some((_, Some(l: LeaseTransaction))) =>
          case Some((_, Some(x: ExchangeTransaction))) =>
          case _ =>
        }
      }

      blockAtHeight
    }
  }

  override def transactionInfo(id: ByteStr) = readOnly(_.opt(k.transactionInfo(id)))

  override def addressTransactions(address: Address, types: Set[TransactionParser.TransactionType.Value], from: Int, count: Int) = ???

  override def paymentTransactionIdByHash(hash: ByteStr) = None

  override def aliasesOfAddress(a: Address) = ???

  override def resolveAlias(a: Alias): Option[Address] = readOnly(_.opt(k.addressOfAlias(a)))

  override def leaseDetails(leaseId: ByteStr) = readOnly(_.opt(k.transactionInfo(leaseId)) match {
    case Some((h, Some(lt: LeaseTransaction))) =>
      Some(LeaseDetails(lt.sender, lt.recipient, h, lt.amount, loadLeaseStatus(leaseId)))
    case _ => None
  })

  override def balanceSnapshots(address: Address, from: Int, to: Int) = ???

  override def leaseOverflows = readOnly { db =>
    addressesWithActiveLeases(db).map { address =>
      val wb = loadWavesBalance(address)
      val lb = loadLeaseBalance(address)
      address -> (lb.out - wb)
    }.filter(_._2 > 0).toMap
  }

  override def leasesOf(address: Address) = ???

  private def activeLeaseTransactions(db: ReadOnlyDB) =
    (1 to height).view
      .flatMap { h => db.get(k.transactionIdsAtHeight(h)) }
      .flatMap { id => db.opt(k.transactionInfo(id)) }
      .collect {
        case (_, Some(lt: LeaseTransaction)) if loadLeaseStatus(lt.id()) => lt
      }

  private def addressesWithActiveLeases(db: ReadOnlyDB) =
    activeLeaseTransactions(db).flatMap { lt =>
      lt.recipient match {
        case a: Address => Seq(lt.sender.toAddress, a)
        case alias: Alias => resolveAlias(alias).fold[Seq[Address]](Seq(lt.sender.toAddress))(a => Seq(lt.sender.toAddress, a))
      }
    }

  override def activeLeases = readOnly(db => activeLeaseTransactions(db).map(_.id()))

  override def nonZeroLeaseBalances = readOnly { db =>
    addressesWithActiveLeases(db).map(address => address -> leaseBalance(address)).toMap
  }

  override def scoreOf(blockId: ByteStr) = ???

  override def blockHeaderAndSize(height: Int) = ???

  override def blockHeaderAndSize(blockId: ByteStr) = ???

  override def blockBytes(height: Int) = ???

  override def blockBytes(blockId: ByteStr) = ???

  override def heightOf(blockId: ByteStr) = readOnly(_.opt(k.heightBySignature(blockId)))

  override def lastBlockIds(howMany: Int) = ???

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override def approvedFeatures() = Map.empty

  override def activatedFeatures() = Map.empty

  override def featureVotes(height: Int) = ???

  override def status = ???
}
