package com.wavesplatform.db

import com.typesafe.config.ConfigFactory
import com.wavesplatform.history.Domain
import com.wavesplatform.settings.{FunctionalitySettings, WavesSettings, loadConfig}
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{BlockchainUpdaterImpl, StateWriter}
import scorex.transaction.History
import scorex.utils.{ScorexLogging, TimeImpl}

trait WithState extends ScorexLogging {
  def withStateAndHistory(fs: FunctionalitySettings)
                         (test: StateWriter with SnapshotStateReader with History => Any): Unit = {
    val state = TestStateWriter(fs)
    try test(state)
    finally state.close()
  }

  def withDomain[A](settings: WavesSettings = WavesSettings.fromConfig(loadConfig(ConfigFactory.load())))
                   (test: Domain => A): A = {
    val stateWriter = TestStateWriter(settings.blockchainSettings.functionalitySettings)
    val time = new TimeImpl
    val bcu = new BlockchainUpdaterImpl(stateWriter, settings, time, stateWriter)
    try { test(Domain(bcu.historyReader, bcu.stateReader, bcu)) }
    finally {
      stateWriter.close()
      time.close()
    }
  }
}
