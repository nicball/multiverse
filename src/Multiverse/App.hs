module Multiverse.App
  ( run
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import Control.Monad (forM_, forever, when)
import Data.Maybe (catMaybes)
import Data.Text (Text)
import Data.Text qualified as Text
import GHC.Records (HasField (getField))
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
      telegram <- startIfEnabled logger "telegram" (Telegram.telegramBridge runtime) config.telegram
      matrix <- startIfEnabled logger "matrix" (Matrix.matrixBridge runtime) config.matrix
      qq <- startIfEnabled logger "qq" (QQ.qqBridge runtime) config.qq
      let bridges = catMaybes [fst <$> telegram, fst <$> matrix, fst <$> qq]
          updatedConfig =
            config
              { telegram = (snd <$> telegram) <|> config.telegram
              , matrix = (snd <$> matrix) <|> config.matrix
              , qq = (snd <$> qq) <|> config.qq
              }
      when (updatedConfig /= config) (writeConfig configPath updatedConfig)
      forM_ bridges (startBridge logger)
      if null bridges
        then logWarn logger "no bridges configured or enabled"
        else forever (threadDelay maxBound)

-- | A disabled bridge is left untouched: its mapping database is not opened,
-- its initial rooms are not ensured, and its config is written back as it was.
startIfEnabled ::
  HasField "enable" bridgeConfig Bool =>
  Logger ->
  Text ->
  (bridgeConfig -> IO a) ->
  Maybe bridgeConfig ->
  IO (Maybe a)
startIfEnabled logger name start = \case
  Nothing -> pure Nothing
  Just bridgeConfig
    | getField @"enable" bridgeConfig -> Just <$> start bridgeConfig
    | otherwise -> do
        logInfo logger (name <> " bridge disabled, skipping")
        pure Nothing

configPathFromArgs :: IO FilePath
configPathFromArgs = do
  args <- getArgs
  pure case args of
    path : _ -> path
    [] -> "multiverse.toml"
