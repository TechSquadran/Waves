package com.wavesplatform.state2.patch

import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{Diff, LeaseBalance, Portfolio}

object CancelAllLeases {
  def apply(s: SnapshotStateReader): Diff = {

    def invertLeaseInfo(l: LeaseBalance): LeaseBalance = LeaseBalance(-l.in, -l.out )

    val portfolioUpd = s.nonZeroLeaseBalances
      .collect { case (acc, pf) if pf != LeaseBalance.empty =>
        acc -> Portfolio(0, invertLeaseInfo(pf), Map.empty)
      }

    Diff(transactions = Map.empty,
      portfolios = portfolioUpd,
      issuedAssets = Map.empty,
      aliases = Map.empty,
      paymentTransactionIdsByHashes = Map.empty,
      orderFills = Map.empty,
      leaseState = s.activeLeases.map(_ -> false).toMap,
      scripts = Map.empty)
  }
}
