package com.wavesplatform.state2

case class BalanceSnapshot(height: Int, regularBalance: Long, leaseIn: Long, leaseOut: Long) {
  lazy val effectiveBalance = regularBalance + leaseIn - leaseOut
}
