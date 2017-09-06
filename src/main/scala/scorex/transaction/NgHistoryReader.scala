package scorex.transaction


import com.wavesplatform.state2._
import scorex.block.Block.BlockId
import scorex.block.{Block, BlockHeader, MicroBlock}
import scorex.transaction.History.{BlockMinerInfo, BlockchainScore}

class NgHistoryReader(ngState: () => Option[NgState], inner: History)
  extends History with NgHistory with DebugNgHistory {

  override def approvedFeatures() = inner.approvedFeatures()
  override def activatedFeatures() = inner.activatedFeatures()
  override def featureVotes(height: Int) = {
    val innerVotes = inner.featureVotes(height)
    ngState() match {
      case Some(ng) if this.height == height =>
        val ngVotes = ng.bestLiquidBlock
          .featureVotes
          .map { featureId => featureId -> (innerVotes.getOrElse(featureId, 0) + 1) }.toMap

        innerVotes  ++ ngVotes
      case _ => innerVotes
    }
  }

  private def liquidBlockHeaderAndSize() = ngState().map { s => (s.bestLiquidBlock, s.bestLiquidBlock.bytes().length) }

  override def blockHeaderAndSize(blockId: BlockId) =
    liquidBlockHeaderAndSize().filter(_._1.uniqueId == blockId) orElse inner.blockHeaderAndSize(blockId)

  override def height: Int = inner.height + ngState().map(_ => 1).getOrElse(0)

  override def blockBytes(height: Int): Option[Array[Byte]] = {
    inner.blockBytes(height).orElse(if (height == inner.height + 1) ngState().map(_.bestLiquidBlock.bytes()) else None)
  }

  override def scoreOf(blockId: BlockId): Option[BlockchainScore] = {
    inner.scoreOf(blockId)
      .orElse(ngState() match {
        case Some(ng) if ng.contains(blockId) => Some(inner.score + ng.base.blockScore())
        case _ => None
      })
  }

  override def heightOf(blockId: BlockId): Option[Int] =
    inner.heightOf(blockId).orElse(ngState().collect { case ng if ng.contains(blockId) => inner.height + 1 })

  override def lastBlockIds(howMany: Int): Seq[BlockId] = {
    ngState() match {
      case Some(ng) =>
        ng.bestLiquidBlockId +: inner.lastBlockIds(howMany - 1)
      case None =>
        inner.lastBlockIds(howMany)
    }
  }

  override def microBlock(id: BlockId): Option[MicroBlock] = {
    for {
      ng <- ngState()
      mb <- ng.microBlock(id)
    } yield mb
  }

  def lastBlockTimestamp: Option[Long] = {
    ngState().map(_.base.timestamp).orElse(inner.lastBlockTimestamp)
  }

  def lastBlockId: Option[AssetId] = {
    ngState().map(_.bestLiquidBlockId).orElse(inner.lastBlockId)
  }

  def blockAt(height: Int): Option[Block] = {
    if (height == inner.height + 1)
      ngState().map(_.bestLiquidBlock)
    else
      inner.blockAt(height)
  }

  override def lastPersistedBlockIds(count: Int): Seq[BlockId] = {
    inner.lastBlockIds(count)
  }

  override def microblockIds(): Seq[BlockId] = {
    ngState().toSeq.flatMap(_.microBlockIds)
  }

  override def bestLastBlockInfo(maxTimestamp: Long): Option[BlockMinerInfo] = {
    ngState().map(_.bestLastBlockInfo(maxTimestamp))
      .orElse(inner.lastBlock.map(b => BlockMinerInfo(b.consensusData, b.timestamp, b.uniqueId)))
  }

  override def score = inner.score + ngState().map(_.bestLiquidBlock.blockScore()).getOrElse(BigInt(0))

  override def lastBlock = ngState().map(_.bestLiquidBlock).orElse(inner.lastBlock)

  override def blockBytes(blockId: ByteStr) = (for {
    ng <- ngState()
    (block, _, _) <- ng.totalDiffOf(blockId)
  } yield block.bytes()).orElse(inner.blockBytes(blockId))

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = {
    if (height == inner.height + 1)
      ngState().map(x => (x.bestLiquidBlock, x.bestLiquidBlock.bytes().length))
    else
      inner.blockHeaderAndSize(height)
  }
}
