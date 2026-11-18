module Multiverse.App
  ( run
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import Control.Monad (forM_, forever, when)
import Data.Maybe (catMaybes)
import Data.Text qualified as Text
import System.Environment (getArgs)
import Multiverse.Bridge
import Multiverse.Config
import Multiverse.Log
import Multiverse.Matrix qualified as Matrix
import Multiverse.QQ qualified as QQ
import Multiverse.SQLite qualified as SQLite
import Multiverse.Telegram qualified as Telegram

run :: IO ()
run = do
  configPath <- configPathFromArgs
  config <- loadConfig configPath
  bracket (newLogger config.logLevel) closeLogger \logger -> do
    logInfo logger ("loaded config " <> Text.pack configPath)
    bracket (SQLite.openTimeline config.timelineDb) SQLite.closeTimeline \timeline -> do
      logInfo logger ("timeline opened " <> Text.pack config.timelineDb)
      let runtime = BridgeRuntime {timeline, logger}
      telegram <- traverse (Telegram.telegramBridge runtime) config.telegram
      matrix <- traverse (Matrix.matrixBridge runtime) config.matrix
      qq <- traverse (QQ.qqBridge runtime) config.qq
      let bridges = catMaybes [fst <$> telegram, fst <$> matrix, fst <$> qq]
          updatedConfig =
            config
              { telegram = snd <$> telegram
              , matrix = snd <$> matrix
              , qq = snd <$> qq
              }
      when (updatedConfig /= config) (writeConfig configPath updatedConfig)
      forM_ bridges (startBridge logger)
      if null bridges
        then logWarn logger "no bridges configured"
        else forever (threadDelay maxBound)

configPathFromArgs :: IO FilePath
configPathFromArgs = do
  args <- getArgs
  pure case args of
    path : _ -> path
    [] -> "multiverse.toml"
