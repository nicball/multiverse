module Multiverse.App
  ( run
  )
where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad (forever, void)
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as Text
import System.Environment (getArgs)
import Multiverse.Bridge
import Multiverse.Config
import Multiverse.Log
import Multiverse.Matrix
import Multiverse.SQLite qualified as SQLite
import Multiverse.Telegram
import Multiverse.Types

run :: IO ()
run = do
  configPath <- configPathFromArgs
  config <- loadConfig configPath
  configRef <- newIORef config
  logger <- newLogger config.logLevel
  logInfo logger ("loaded config " <> Text.pack configPath)
  timeline <- SQLite.openTimeline config.timelineDb
  logInfo logger ("timeline opened " <> Text.pack config.timelineDb)
  telegramStarted <- maybeStartTelegram configPath configRef logger timeline config.telegram
  matrixStarted <- maybeStartMatrix configPath configRef logger timeline config.matrix
  if telegramStarted || matrixStarted
    then forever (threadDelay maxBound)
    else logWarn logger "no bridges configured"

maybeStartTelegram :: FilePath -> IORef AppConfig -> Logger -> SQLite.SQLiteTimeline -> Maybe TelegramBridgeConfig -> IO Bool
maybeStartTelegram configPath configRef logger timeline telegramConfig =
  case telegramConfig of
    Nothing -> pure False
    Just bridgeConfig -> do
      mappingStore <- openMappingStore bridgeConfig.mappingDb
      let telegramRuntimeConfig =
            TelegramConfig
              { botToken = bridgeConfig.botToken
              , initialRooms = bridgeConfig.initialRooms
              , pollTimeoutSeconds = bridgeConfig.pollTimeoutSeconds
              , noteInitialRoom = writeInitialRoom configPath configRef "telegram"
              }
          bridge = telegramBridge telegramRuntimeConfig
          context = BridgeContext {timeline, mappingStore, logger}
      startComponent logger "telegram observer" (bridge.observe context)
      startComponent logger "telegram reflector" (bridge.reflect context)
      logInfo logger ("telegram bridge started with mapping db " <> Text.pack bridgeConfig.mappingDb)
      pure True

maybeStartMatrix :: FilePath -> IORef AppConfig -> Logger -> SQLite.SQLiteTimeline -> Maybe MatrixBridgeConfig -> IO Bool
maybeStartMatrix configPath configRef logger timeline matrixConfig =
  case matrixConfig of
    Just bridgeConfig -> do
      mappingStore <- openMappingStore bridgeConfig.mappingDb
      let matrixRuntimeConfig =
            MatrixConfig
              { homeserver = bridgeConfig.homeserver
              , accessToken = bridgeConfig.accessToken
              , initialRooms = bridgeConfig.initialRooms
              , syncTimeoutMs = bridgeConfig.syncTimeoutMs
              , noteInitialRoom = writeInitialRoom configPath configRef "matrix"
              }
          bridge = matrixBridge matrixRuntimeConfig
          context = BridgeContext {timeline, mappingStore, logger}
      startComponent logger "matrix observer" (bridge.observe context)
      startComponent logger "matrix reflector" (bridge.reflect context)
      logInfo logger ("matrix bridge started with mapping db " <> Text.pack bridgeConfig.mappingDb)
      pure True
    Nothing -> pure False

configPathFromArgs :: IO FilePath
configPathFromArgs = do
  args <- getArgs
  pure case args of
    path : _ -> path
    [] -> "multiverse.toml"

writeInitialRoom :: FilePath -> IORef AppConfig -> Text -> InitialRoomMapping -> RoomId -> IO ()
writeInitialRoom configPath configRef bridgeName mapping room = do
  config <-
    atomicModifyIORef' configRef \config0 ->
      let config1 = setInitialRoom bridgeName mapping.platformKey room config0
       in (config1, config1)
  writeConfig configPath config

startComponent :: Logger -> Text -> IO () -> IO ()
startComponent logger name component =
  void (forkIO (forever runOnce))
 where
  runOnce = do
    result <- try component :: IO (Either SomeException ())
    case result of
      Left err -> logError logger (name <> " crashed, restarting: " <> Text.pack (show err))
      Right () -> logWarn logger (name <> " stopped, restarting")
    threadDelay 5000000
