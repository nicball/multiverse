module Multiverse.Log
  ( Logger
  , LogLevel
  , Severity (..)
  , newLogger
  , closeLogger
  , logDebug
  , logInfo
  , logWarn
  , logError
  )
where

import Data.Aeson (object)
import Data.Text (Text)
import Katip
import System.IO (stdout)

type Logger = LogEnv

type LogLevel = Severity

newLogger :: LogLevel -> IO Logger
newLogger level = do
  scribe <- mkHandleScribe ColorIfTerminal stdout (permitItem level) V2
  logEnv <- initLogEnv "multiverse" "production"
  registerScribe "stdout" scribe defaultScribeSettings logEnv

closeLogger :: Logger -> IO ()
closeLogger logger = do
  _ <- closeScribes logger
  pure ()

logDebug :: Logger -> Text -> IO ()
logDebug = logAt DebugS

logInfo :: Logger -> Text -> IO ()
logInfo = logAt InfoS

logWarn :: Logger -> Text -> IO ()
logWarn = logAt WarningS

logError :: Logger -> Text -> IO ()
logError = logAt ErrorS

logAt :: Severity -> Logger -> Text -> IO ()
logAt severity logger message =
  runKatipContextT logger (sl "context" (object [])) "multiverse" do
    logMsg "multiverse" severity (ls message)
