package com.wavesplatform.state2.patch

import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{Diff, LeaseBalance, Portfolio}

object CancelLeaseOverflow {
  def apply(s: SnapshotStateReader): Diff = {

    val portfolioUpd = s.leaseOverflows.mapValues(v => Portfolio(0, LeaseBalance(0, -v), Map.empty))

    val cancelledLeases = for {
      addr <- portfolioUpd.keys
      (leaseId, ld) <- s.leasesOf(addr)
      if ld.isActive
    } yield (leaseId, false)

    Diff(transactions = Map.empty,
      portfolios = portfolioUpd,
      issuedAssets = Map.empty,
      aliases = Map.empty,
      paymentTransactionIdsByHashes = Map.empty,
      orderFills = Map.empty,
      leaseState = cancelledLeases.toMap,
      scripts = Map.empty)
  }
}
