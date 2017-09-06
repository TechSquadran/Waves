package com.wavesplatform.state2

import com.wavesplatform.state2.StateWriter.Status
import monix.reactive.Observable
import scorex.block.Block

trait StateWriter {
  def append(diff: Diff, block: Block): Unit
  def rollbackTo(targetBlockId: ByteStr): Seq[Block]
  def status: Observable[Status]
}

object StateWriter {
  case class Status(height: Int, lastUpdated: Long)
}