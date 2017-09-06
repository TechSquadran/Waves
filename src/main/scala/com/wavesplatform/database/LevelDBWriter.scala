package com.wavesplatform.database

import java.nio.ByteBuffer

import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.google.common.primitives.{Ints, Longs}
import com.wavesplatform.state2._
import com.wavesplatform.state2.reader.LeaseDetails
import org.iq80.leveldb.{DB, ReadOptions}
import scalikejdbc.using
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
    def apply[V](key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): Key[V] = new Key[V] {
      override def keyBytes = key
      override def parse(bytes: Array[Byte]) = parser(bytes)
      override def encode(v: V) = encoder(v)
    }

    def opt[V](key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): Key[Option[V]] =
      apply[Option[V]](key, Option(_).map(parser), _.fold[Array[Byte]](null)(encoder))
  }

  object k {
    private def h(prefix: Int, height: Int): Array[Byte] = {
      val ndo = newDataOutput(6)
      ndo.writeShort(prefix)
      ndo.writeInt(height)
      ndo.toByteArray
    }

    private def byteKeyWithH(prefix: Int, height: Int, bytes: Array[Byte]) = {
      val ndo = newDataOutput(6 + bytes.length)
      ndo.writeShort(prefix)
      ndo.writeInt(height)
      ndo.write(bytes)
      ndo.toByteArray
    }

    private def byteKey(prefix: Int, bytes: Array[Byte]) = {
      val ndo = newDataOutput(2 + bytes.length)
      ndo.writeShort(prefix)
      ndo.write(bytes)
      ndo.toByteArray
    }

    private def addr(prefix: Int, address: Address) = byteKey(prefix, address.bytes.arr)
    private def hash(prefix: Int, hashBytes: ByteStr) = byteKey(prefix, hashBytes.arr)

    private def addressWithH(prefix: Int, height: Int, address: Address): Array[Byte] = {
      val ndo = newDataOutput(6 + address.bytes.arr.length)
      ndo.writeShort(prefix)
      ndo.writeInt(height)
      ndo.write(address.bytes.arr)
      ndo.toByteArray
    }

    private def writeIntSeq(values: Seq[Int]): Array[Byte] = {
      val ndo = newDataOutput()
      ndo.writeInt(values.length)
      values.foreach(ndo.writeInt)
      ndo.toByteArray
    }

    private def readIntSeq(data: Array[Byte]): Seq[Int] = Option(data).fold(Seq.empty[Int]) { d =>
      val ndi = newDataInput(d)
      (1 to ndi.readInt()).map(_ => ndi.readInt())
    }

    private def readByteStrSeq(chunkSize: Int)(data: Array[Byte]): Seq[ByteStr] = Option(data).fold(Seq.empty[ByteStr]) { d =>
      val ndi = newDataInput(d)
      (0 until ndi.readInt()).map { _ =>
        val bytes = new Array[Byte](chunkSize)
        ndi.readFully(bytes)
        ByteStr(bytes)
      }
    }

    private def writeByteStrSeq(values: Seq[ByteStr]) = {
      val ndo = newDataOutput()
      ndo.writeInt(values.length)
      values.view.map(_.arr).foreach(ndo.write)
      ndo.toByteArray
    }

    private def historyKey(prefix: Int, bytes: Array[Byte]) = Key(byteKey(prefix, bytes), readIntSeq, writeIntSeq)

    val height: Key[Int] =
      Key[Int](Array(0, 1), Option(_).fold(0)(Ints.fromByteArray), Ints.toByteArray)

    def score(height: Int) =
      Key[BigInt](h(2, height), Option(_).fold(BigInt(0))(BigInt(_)), _.toByteArray)

    def blockAt(height: Int): Key[Option[Block]] = Key.opt[Block](h(3, height), Block.parseBytes(_).get, _.bytes())
    def heightBySignature(blockId: ByteStr): Key[Option[Int]] = Key.opt(hash(4, blockId), Ints.fromByteArray, Ints.toByteArray)

    def wavesBalanceHistory(address: Address): Key[Seq[Int]] = historyKey(5, address.bytes.arr)

    def wavesBalance(height: Int, address: Address): Key[Long] =
      Key(addressWithH(6, height, address), Option(_).fold(0L)(Longs.fromByteArray), Longs.toByteArray)

    def assetList(address: Address): Key[Set[ByteStr]] = Key(addr(7, address), readByteStrSeq(32)(_).toSet, assets => writeByteStrSeq(assets.toSeq))
    def assetBalanceHistory(address: Address, assetId: ByteStr): Key[Seq[Int]] = historyKey(8, address.bytes.arr ++ assetId.arr)
    def assetBalance(height: Int, address: Address, assetId: ByteStr): Key[Long] =
      Key(byteKeyWithH(9, height, address.bytes.arr ++ assetId.arr), Option(_).fold(0L)(Longs.fromByteArray), Longs.toByteArray)

    private def readAssetInfo(data: Array[Byte]) = {
      val b = ByteBuffer.wrap(data)
      AssetInfo(b.get() == 1, b.getLong)
    }

    private def writeAssetInfo(ai: AssetInfo) =
      ByteBuffer.allocate(1 + 8).put((if (ai.isReissuable) 1 else 0): Byte).putLong(ai.volume.longValue()).array()

    def assetInfoHistory(assetId: ByteStr): Key[Seq[Int]] = historyKey(10, assetId.arr)
    def assetInfo(height: Int, assetId: ByteStr): Key[AssetInfo] = Key(byteKeyWithH(11, height, assetId.arr), readAssetInfo, writeAssetInfo)

    private def writeLeaseBalance(lb: LeaseBalance): Array[Byte] = {
      val ndo = newDataOutput()
      ndo.writeLong(lb.in)
      ndo.writeLong(lb.out)
      ndo.toByteArray
    }

    private def readLeaseBalance(data: Array[Byte]) = Option(data).fold(LeaseBalance.empty) { d =>
      val ndi = newDataInput(d)
      LeaseBalance(ndi.readLong(), ndi.readLong())
    }

    def leaseBalanceHistory(address: Address): Key[Seq[Int]] = historyKey(12, address.bytes.arr)
    def leaseBalance(height: Int, address: Address): Key[LeaseBalance] = Key(byteKeyWithH(13, height, address.bytes.arr), readLeaseBalance, writeLeaseBalance)

    def leaseStatusHistory(leaseId: ByteStr): Key[Seq[Int]] = historyKey(14, leaseId.arr)
    def leaseStatus(height: Int, leaseId: ByteStr): Key[Boolean] = Key(byteKeyWithH(15, height, leaseId.arr), _(0) == 1, active => Array[Byte](if (active) 1 else 0))

    def filledVolumeAndFeeHistory(orderId: ByteStr): Key[Seq[Int]] = historyKey(16, orderId.arr)

    private def readVolumeAndFee(data: Array[Byte]) = Option(data).fold(VolumeAndFee.empty) { d =>
      val ndi = newDataInput(d)
      VolumeAndFee(ndi.readLong(), ndi.readLong())
    }

    private def writeVolumeAndFee(vf: VolumeAndFee) = {
      val ndo = newDataOutput()
      ndo.writeLong(vf.volume)
      ndo.writeLong(vf.fee)
      ndo.toByteArray
    }

    def filledVolumeAndFee(height: Int, orderId: ByteStr): Key[VolumeAndFee] =
      Key(byteKeyWithH(17, height, orderId.arr), readVolumeAndFee, writeVolumeAndFee)


    private def readTransactionInfo(data: Array[Byte]) =
      (Ints.fromByteArray(data), if (data.length == 4) None else Some(TransactionParser.parseBytes(data.drop(4)).get))

    private def writeTransactionInfo(txInfo: (Int, Option[Transaction])) = txInfo match {
      case (h, Some(tx)) =>
        val txBytes = tx.bytes()
        ByteBuffer.allocate(4 + txBytes.length).putInt(h).put(txBytes).array()
      case (h, None) => Ints.toByteArray(h)
    }

    def transactionInfo(txId: ByteStr): Key[Option[(Int, Option[Transaction])]] = Key.opt(hash(18, txId), readTransactionInfo, writeTransactionInfo)

    def addressTransactionHistory(address: Address): Key[Seq[Int]] = historyKey(19, address.bytes.arr)
    def addressTransactionIds(height: Int, address: Address): Key[Seq[ByteStr]] = ???


    def changedAddresses(height: Int): Key[Seq[Address]] = Key(h(20, height),
      readByteStrSeq(Address.AddressLength)(_).map(bs => Address.fromBytes(bs.arr).explicitGet()),
      addresses => writeByteStrSeq(addresses.map(_.bytes)))
    def transactionIdsAtHeight(height: Int): Key[Seq[ByteStr]] = Key(h(21, height), readByteStrSeq(32), writeByteStrSeq)

    def addressOfAlias(alias: Alias): Key[Option[Address]] = Key.opt(byteKey(22, alias.bytes.arr), Address.fromBytes(_).explicitGet(), _.bytes.arr)
  }

  class ReadOnlyDB(db: DB, readOptions: ReadOptions) {
    def get[V](key: Key[V]): V = key.parse(db.get(key.keyBytes))
  }

  class RW(db: DB) extends AutoCloseable {
    private val batch = db.createWriteBatch()
    private val snapshot = db.getSnapshot
    private val readOptions = new ReadOptions().snapshot(snapshot)

    def get[V](key: Key[V]): V = key.parse(db.get(key.keyBytes, readOptions))

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

  private def readWrite[A](f: RW => A): A = using(new RW(writableDB)) { rw => f(rw) }

  override protected def loadAddressId(address: Address): BigInt = ???

  override protected def loadHeight(): Int = readOnly(_.get(k.height))

  override protected def loadScore(): BigInt = readOnly { db => db.get(k.score(db.get(k.height))) }

  override protected def loadLastBlock(): Option[Block] = readOnly { db => db.get(k.blockAt(db.get(k.height))) }

  override protected def loadWavesBalance(address: Address): Long = readOnly { db =>
    db.get(k.wavesBalanceHistory(address)).headOption.fold(0L)(h => db.get(k.wavesBalance(h, address)))
  }

  override protected def loadLeaseBalance(address: Address): LeaseBalance = readOnly { db =>
    db.get(k.leaseBalanceHistory(address)).headOption.fold(LeaseBalance.empty)(h => db.get(k.leaseBalance(h, address)))
  }

  override protected def loadAssetBalance(address: Address): Map[ByteStr, Long] = readOnly { db =>
    (for {
      assetId <- db.get(k.assetList(address))
      h <- db.get(k.assetBalanceHistory(address, assetId)).headOption
    } yield assetId -> db.get(k.assetBalance(h, address, assetId))).toMap
  }

  override protected def loadAssetInfo(assetId: ByteStr): Option[AssetInfo] = readOnly(LevelDBWriter.loadAssetInfo(_, assetId))

  override protected def loadAssetDescription(assetId: ByteStr): Option[AssetDescription] = readOnly { db =>
    db.get(k.transactionInfo(assetId)) match {
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

  private def updateHistory(rw: RW, key: Key[Seq[Int]], threshold: Int, kf: Int => Key[_]): Seq[Array[Byte]] = {
    val (c1, c2) = rw.get(key).partition(_ > threshold)
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
    rw.put(k.blockAt(height), Some(block))

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
        case ca: CreateAliasTransaction => rw.put(k.addressOfAlias(ca.alias), Some(ca.sender.toAddress))
        case _ =>
      }

      rw.put(k.transactionInfo(id), Some((height, txToSave)))
    }

    rw.put(k.transactionIdsAtHeight(height), transactions.keys.toSeq)
    expiredKeys.foreach(rw.delete)
  }

  override protected def doRollback(targetBlockId: ByteStr): Seq[Block] = readWrite { rw =>
    rw.get(k.heightBySignature(targetBlockId)).fold(Seq.empty[Int])(_ to height).flatMap { h =>
      val blockAtHeight = rw.get(k.blockAt(h))
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
        rw.get(k.transactionInfo(txId)) match {
          case Some((_, Some(i: IssueTransaction))) =>
          case Some((_, Some(r: ReissueTransaction))) =>
          case Some((_, Some(c: LeaseCancelTransaction))) =>
          case Some((_, Some(l: LeaseTransaction))) =>
          case Some((_, Some(x: ExchangeTransaction))) =>
        }
      }

      blockAtHeight
    }
  }

  override def transactionInfo(id: ByteStr) = readOnly(db => db.get(k.transactionInfo(id)))

  override def addressTransactions(address: Address, types: Set[TransactionParser.TransactionType.Value], from: Int, count: Int) = ???

  override def paymentTransactionIdByHash(hash: ByteStr) = None

  override def aliasesOfAddress(a: Address) = ???

  override def resolveAlias(a: Alias): Option[Address] = readOnly(db => db.get(k.addressOfAlias(a)))

  override def leaseDetails(leaseId: ByteStr) = readOnly { db =>
    db.get(k.transactionInfo(leaseId)) match {
      case Some((h, Some(lt: LeaseTransaction))) =>
        Some(LeaseDetails(lt.sender, lt.recipient, h, lt.amount, loadLeaseStatus(leaseId)))
      case _ => None
    }
  }

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
      .flatMap { h =>
        db.get(k.transactionIdsAtHeight(h))
      }
      .flatMap { id =>
        db.get(k.transactionInfo(id)) }
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

  override def heightOf(blockId: ByteStr) = readOnly(_.get(k.heightBySignature(blockId)))

  override def lastBlockIds(howMany: Int) = ???

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override def approvedFeatures() = Map.empty

  override def activatedFeatures() = Map.empty

  override def featureVotes(height: Int) = ???

  override def status = ???
}
