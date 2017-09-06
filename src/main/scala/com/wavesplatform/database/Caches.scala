package com.wavesplatform.database

import java.util

import cats.syntax.monoid._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{AssetDescription, AssetInfo, ByteStr, Diff, LeaseBalance, StateWriter, VolumeAndFee}
import scorex.account.Address
import scorex.block.Block
import scorex.transaction.{History, Transaction}
import scorex.transaction.assets.IssueTransaction

trait Caches extends SnapshotStateReader with History with StateWriter {
  import Caches._

  @volatile
  private var heightCache = loadHeight()
  protected def loadHeight(): Int
  override def height = heightCache

  @volatile
  private var scoreCache = loadScore()
  protected def loadScore(): BigInt
  override def score = scoreCache

  @volatile
  private var lastBlockCache = loadLastBlock()
  protected def loadLastBlock(): Option[Block]
  override def lastBlock = lastBlockCache

  private val recentTransactionIds = new util.HashMap[ByteStr, Long]()
  override def containsTransaction(id: ByteStr) = recentTransactionIds.containsKey(id)

  protected val wavesBalanceCache: LoadingCache[Address, java.lang.Long] = cache(100000, loadWavesBalance)
  protected def loadWavesBalance(address: Address): Long
  override def wavesBalance(address: Address) = wavesBalanceCache.get(address)

  protected val leaseBalanceCache: LoadingCache[Address, LeaseBalance] = cache(100000, loadLeaseBalance)
  protected def loadLeaseBalance(address: Address): LeaseBalance
  override def leaseBalance(address: Address) = leaseBalanceCache.get(address, () => loadLeaseBalance(address))

  protected val assetBalanceCache: LoadingCache[Address, Map[ByteStr, Long]] = cache(100000, loadAssetBalance)
  protected def loadAssetBalance(address: Address): Map[ByteStr, Long]
  override def assetBalance(address: Address) = assetBalanceCache.get(address)

  protected val assetInfoCache: LoadingCache[ByteStr, Option[AssetInfo]] = cache(100000, loadAssetInfo)
  protected def loadAssetInfo(assetId: ByteStr): Option[AssetInfo]

  protected val assetDescriptionCache: LoadingCache[ByteStr, Option[AssetDescription]] = cache(100000, loadAssetDescription)
  protected def loadAssetDescription(assetId: ByteStr): Option[AssetDescription]
  override def assetDescription(assetId: ByteStr) = assetDescriptionCache.get(assetId)

  protected val volumeAndFeeCache: LoadingCache[ByteStr, VolumeAndFee] = cache(100000, loadVolumeAndFee)
  protected def loadVolumeAndFee(orderId: ByteStr): VolumeAndFee
  override def filledVolumeAndFee(orderId: ByteStr) = volumeAndFeeCache.get(orderId)

  protected val addressIdCache: LoadingCache[Address, BigInt] = cache(100000, loadAddressId)
  protected def loadAddressId(address: Address): BigInt

  private val transactionIds = new util.HashMap[ByteStr, Long]()

  protected def doAppend(block: Block,
                        wavesBalances: Map[Address, Long],
                        assetBalances: Map[Address, Map[ByteStr, Long]],
                        leaseBalances: Map[Address, LeaseBalance],
                        leaseStates: Map[ByteStr, Boolean],
                        transactions: Map[ByteStr, (Transaction, Set[Address])],
                        reissuedAssets: Map[ByteStr, AssetInfo],
                        filledQuantity: Map[ByteStr, VolumeAndFee]): Unit

  override def append(diff: Diff, block: Block): Unit = {
    heightCache += 1
    scoreCache += block.blockScore()
    lastBlockCache = Some(block)

    transactionIds.entrySet().removeIf(kv => block.timestamp - kv.getValue > 2 * 60 * 60 * 1000)

    val wavesBalances = Map.newBuilder[Address, Long]
    val assetBalances = Map.newBuilder[Address, Map[ByteStr, Long]]
    val leaseBalances = Map.newBuilder[Address, LeaseBalance]

    for ((address, portfolio) <- diff.portfolios) {
      if (portfolio.balance != 0) {
        val newBalance = wavesBalanceCache.get(address) + portfolio.balance
        wavesBalanceCache.put(address, newBalance)
        wavesBalances += address -> newBalance
      }

      if (portfolio.lease != LeaseBalance.empty) {
        val newLeaseBalance = portfolio.lease.combine(leaseBalanceCache.get(address))
        leaseBalanceCache.put(address, newLeaseBalance)
        leaseBalances += address -> newLeaseBalance
      }

      if (portfolio.assets.nonEmpty) {
        val cachedPortfolio = assetBalance(address).withDefaultValue(0L)
        val newPortfolio = for { (k, v) <- portfolio.assets if v != 0 } yield k -> (v + cachedPortfolio(k))
        if (newPortfolio.nonEmpty) {
          assetBalanceCache.put(address, cachedPortfolio ++ newPortfolio)
          assetBalances += address -> newPortfolio
        }
      }
    }

    val newFills = Map.newBuilder[ByteStr, VolumeAndFee]

    for ((orderId, fillInfo) <- diff.orderFills) {
      val newVolumeAndFee = volumeAndFeeCache.get(orderId).combine(fillInfo)
      volumeAndFeeCache.put(orderId, newVolumeAndFee)
      newFills += orderId -> newVolumeAndFee
    }

    val newTransactions = Map.newBuilder[ByteStr, (Transaction, Set[Address])]
    for ((id, (_, tx, addresses)) <- diff.transactions) {
      transactionIds.put(id, tx.timestamp)
      newTransactions += id -> (tx, addresses)
    }

    for ((id, ai) <- diff.issuedAssets) {
      assetInfoCache.put(id, Some(ai))
      diff.transactions.get(id) match {
        case Some((_, it: IssueTransaction, _)) =>
          assetDescriptionCache.put(id, Some(AssetDescription(it.sender, it.name, it.decimals, ai.isReissuable)))
        case _ =>
      }
    }

    doAppend(block, wavesBalances.result(), assetBalances.result(), leaseBalances.result(), diff.leaseState,
      newTransactions.result(), diff.issuedAssets, newFills.result())
  }

  protected def doRollback(targetBlockId: ByteStr): Seq[Block]

  override def rollbackTo(targetBlockId: ByteStr) = {
    val discardedBlocks = doRollback(targetBlockId)

    heightCache = loadHeight()
    scoreCache = loadScore()
    lastBlockCache = loadLastBlock()

    discardedBlocks
  }
}

object Caches {
  def cache[K <: AnyRef, V <: AnyRef](maximumSize: Int, loader: K => V) =
    CacheBuilder.newBuilder().maximumSize(maximumSize).recordStats().build(new CacheLoader[K, V] {
      override def load(key: K) = loader(key)
    })
}
