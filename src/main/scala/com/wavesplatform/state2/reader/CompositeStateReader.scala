package com.wavesplatform.state2.reader

import cats.implicits._
import cats.kernel.Monoid
import com.wavesplatform.state2._
import scorex.account.{Address, Alias}
import scorex.transaction.{Transaction, TransactionParser}
import scorex.transaction.assets.IssueTransaction
import scorex.transaction.lease.LeaseTransaction
import scorex.transaction.smart.Script

class CompositeStateReader(inner: SnapshotStateReader, maybeDiff: => Option[Diff]) extends SnapshotStateReader {

  private def diff = maybeDiff.getOrElse(Diff.empty)

  override def assetDescription(id: ByteStr) = {
    inner
      .assetDescription(id).orElse(diff.transactions.get(id).collectFirst {
        case (_, it: IssueTransaction, _) => AssetDescription(it.sender, it.name, it.decimals, it.reissuable)
      })
      .map(z => diff.issuedAssets.get(id).fold(z)(r => z.copy(reissuable = r.isReissuable)))
  }

  override def leaseOverflows = {
    val innerAddressesWithOverflows = inner.leaseOverflows.keys
    val addressesWithNewLeases = diff.portfolios.collect {
      case (address, portfolio) if portfolio.lease.out > 0 => address
    }

    (innerAddressesWithOverflows ++ addressesWithNewLeases)
      .map(a => a -> (wavesBalance(a) - leaseBalance(a).out))
      .filter { case (a, overflow) => overflow < 0 }
      .toMap
  }

  override def leasesOf(address: Address) = {
    val innerLeases = inner.leasesOf(address)
    val leaseChanges = diff.leaseState.flatMap {
      case (id, false) => Some(id -> innerLeases(id).copy(isActive = false))
      case (id, true) =>
        diff.transactions.get(id) match {
          case Some((h, lt: LeaseTransaction, _)) => Some(id -> LeaseDetails(lt.sender, lt.recipient, h, lt.amount, true))
          case _ => None
        }
    }

    innerLeases ++ leaseChanges
  }

  override def nonZeroLeaseBalances = inner.nonZeroLeaseBalances ++ diff.portfolios.collect {
    case (addr, p) if p.lease != LeaseBalance.empty => addr -> p.lease
  }

  override def leaseDetails(leaseId: ByteStr) = diff.transactions.get(leaseId) match {
    case Some((h, l: LeaseTransaction, _)) => Some(LeaseDetails(l.sender, l.recipient, h, l.amount, diff.leaseState.getOrElse(leaseId, false)))
    case _ => inner.leaseDetails(leaseId)
  }

  override def leaseBalance(a: Address) = Monoid.combine(diff.portfolios.get(a).fold(Monoid.empty[LeaseBalance])(p => p.lease), inner.leaseBalance(a))

  override def transactionInfo(id: ByteStr): Option[(Int, Option[Transaction])] =
    diff.transactions.get(id)
      .map(t => (t._1, Some(t._2)))
      .orElse(inner.transactionInfo(id))

  override def height: Int = inner.height + (if (maybeDiff.isDefined) 1 else 0)

  override def addressTransactions(address: Address,
                                   types: Set[TransactionParser.TransactionType.Value],
                                   from: Int,
                                   count: Int) = {
    val transactionsFromDiff = diff.transactions.values.view.collect {
      case (height, tx, addresses) if addresses(address) && (types(tx.transactionType) || types.isEmpty) => (height, tx)
    }.slice(from, from + count).toSeq

    val actualTxCount = transactionsFromDiff.length

    if (actualTxCount == count) transactionsFromDiff else {
      transactionsFromDiff ++ inner.addressTransactions(address, types, 0, count - actualTxCount)
    }
  }

  override def wavesBalance(a: Address) = {
    val innerBalance = inner.wavesBalance(a)
    diff.portfolios.get(a).fold(innerBalance)(_.balance + innerBalance)
  }


  override def assetBalance(a: Address) = {
    Monoid.combine(inner.assetBalance(a), diff.portfolios.get(a).fold(Map.empty[ByteStr, Long])(_.assets))
  }

  override def paymentTransactionIdByHash(hash: ByteStr): Option[ByteStr]
  = diff.paymentTransactionIdsByHashes.get(hash)
    .orElse(inner.paymentTransactionIdByHash(hash))

  override def aliasesOfAddress(a: Address): Seq[Alias] =
    diff.aliases.filter(_._2 == a).keys.toSeq ++ inner.aliasesOfAddress(a)

  override def resolveAlias(a: Alias): Option[Address] = diff.aliases.get(a).orElse(inner.resolveAlias(a))

  override def activeLeases: Seq[ByteStr] = {
    diff.leaseState.collect { case (id, isActive) if isActive => id }.toSeq ++ inner.activeLeases
  }

  override def containsTransaction(id: ByteStr): Boolean = diff.transactions.contains(id) || inner.containsTransaction(id)

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee =
    diff.orderFills.get(orderId).orEmpty.combine(inner.filledVolumeAndFee(orderId))

  override def balanceSnapshots(address: Address, from: Int, to: Int) = {
    if (to <= inner.height || maybeDiff.isEmpty) inner.balanceSnapshots(address, from, to) else {
      val latestLeaseInfo = leaseBalance(address)
      val wavesB = wavesBalance(address)
      inner.balanceSnapshots(address, from, to) :+
        BalanceSnapshot(height, wavesB, latestLeaseInfo.in, latestLeaseInfo.out)
    }
  }

  override def accountScript(address: Address): Option[Script] = {
    diff.scripts.get(address) match {
      case None => inner.accountScript(address)
      case Some(None) => None
      case Some(Some(scr)) => Some(scr)
    }
  }
}

object CompositeStateReader {
  def composite(inner: SnapshotStateReader, diff: => Option[Diff]): SnapshotStateReader = new CompositeStateReader(inner, diff)
  def composite(inner: SnapshotStateReader, diff: Diff): SnapshotStateReader = new CompositeStateReader(inner, Some(diff))
}
