module Multiverse.Config
  ( AppConfig (..)
  , InitialRoomMapping (..)
  , MatrixBridgeConfig (..)
  , TelegramBridgeConfig (..)
  , QQBridgeConfig (..)
  , loadConfig
  , writeConfig
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import System.Directory (doesFileExist)
import Toml (TomlCodec, (.=))
import Toml qualified
import Multiverse.Log
import Multiverse.Types

data AppConfig = AppConfig
  { timelineDb :: FilePath
  , logLevel :: LogLevel
  , telegram :: Maybe TelegramBridgeConfig
  , matrix :: Maybe MatrixBridgeConfig
  , qq :: Maybe QQBridgeConfig
  }
  deriving (Eq, Show)

data InitialRoomMapping = InitialRoomMapping
  { platformKey :: PlatformKey
  , timelineRoom :: Maybe RoomId
  , enable :: Bool
  }
  deriving (Eq, Show)

data TelegramBridgeConfig = TelegramBridgeConfig
  { botToken :: Text
  , mappingDb :: FilePath
  , pollTimeoutSeconds :: Int
  , initialRooms :: [InitialRoomMapping]
  }
  deriving (Eq, Show)

data MatrixBridgeConfig = MatrixBridgeConfig
  { homeserver :: Text
  , accessToken :: Text
  , mappingDb :: FilePath
  , syncTimeoutMs :: Int
  , initialRooms :: [InitialRoomMapping]
  }
  deriving (Eq, Show)

data QQBridgeConfig = QQBridgeConfig
  { apiUrl :: Text
  , websocketUrl :: Text
  , accessToken :: Text
  , mappingDb :: FilePath
  , initialRooms :: [InitialRoomMapping]
  }
  deriving (Eq, Show)

loadConfig :: FilePath -> IO AppConfig
loadConfig path = do
  exists <- doesFileExist path
  if exists
    then do
      decoded <- Toml.decodeFileEither appConfigCodec path
      case decoded of
        Right config -> pure config
        Left errs -> fail (Text.unpack (Toml.prettyTomlDecodeErrors errs))
    else pure defaultConfig

writeConfig :: FilePath -> AppConfig -> IO ()
writeConfig path config = do
  _ <- Toml.encodeToFile appConfigCodec path config
  pure ()

defaultConfig :: AppConfig
defaultConfig =
  AppConfig
    { timelineDb = "multiverse.sqlite3"
    , logLevel = InfoS
    , telegram = Nothing
    , matrix = Nothing
    , qq = Nothing
    }

appConfigCodec :: TomlCodec AppConfig
appConfigCodec =
  AppConfig
    <$> filePathCodec "timeline_db" .= (.timelineDb)
    <*> optionalWithDefault InfoS (Toml.table logLevelCodec "logging") .= (.logLevel)
    <*> Toml.dioptional (Toml.table telegramConfigCodec "telegram") .= (.telegram)
    <*> Toml.dioptional (Toml.table matrixConfigCodec "matrix") .= (.matrix)
    <*> Toml.dioptional (Toml.table qqConfigCodec "qq") .= (.qq)

telegramConfigCodec :: TomlCodec TelegramBridgeConfig
telegramConfigCodec =
  TelegramBridgeConfig
    <$> Toml.text "bot_token" .= (.botToken)
    <*> optionalWithDefault "telegram-mapping.sqlite3" (filePathCodec "mapping_db") .= (.mappingDb)
    <*> optionalWithDefault 30 (Toml.int "poll_timeout_seconds") .= (.pollTimeoutSeconds)
    <*> optionalWithDefault [] (Toml.list (initialRoomCodec "telegram") "rooms") .= (.initialRooms)

matrixConfigCodec :: TomlCodec MatrixBridgeConfig
matrixConfigCodec =
  MatrixBridgeConfig
    <$> Toml.text "homeserver" .= (.homeserver)
    <*> Toml.text "access_token" .= (.accessToken)
    <*> optionalWithDefault "matrix-mapping.sqlite3" (filePathCodec "mapping_db") .= (.mappingDb)
    <*> optionalWithDefault 30000 (Toml.int "sync_timeout_ms") .= (.syncTimeoutMs)
    <*> optionalWithDefault [] (Toml.list (initialRoomCodec "matrix") "rooms") .= (.initialRooms)

qqConfigCodec :: TomlCodec QQBridgeConfig
qqConfigCodec =
  QQBridgeConfig
    <$> Toml.text "api_url" .= (.apiUrl)
    <*> Toml.text "websocket_url" .= (.websocketUrl)
    <*> Toml.text "access_token" .= (.accessToken)
    <*> optionalWithDefault "qq-mapping.sqlite3" (filePathCodec "mapping_db") .= (.mappingDb)
    <*> optionalWithDefault [] (Toml.list (initialRoomCodec "qq") "rooms") .= (.initialRooms)

initialRoomCodec :: Text -> TomlCodec InitialRoomMapping
initialRoomCodec platform =
  InitialRoomMapping
    <$> platformKeyCodec platform .= (.platformKey)
    <*> Toml.dioptional roomIdCodec .= (.timelineRoom)
    <*> optionalWithDefault True (Toml.bool "enable") .= (.enable)

platformKeyCodec :: Text -> TomlCodec PlatformKey
platformKeyCodec platform =
  Toml.dimap
    (.key)
    (\key -> PlatformKey {platform, entityType = "room", key})
    (Toml.text "platform_key")

roomIdCodec :: TomlCodec RoomId
roomIdCodec =
  Toml.dimap
    renderRoomId
    (RoomId . EventId)
    (Toml.text "timeline_room")

logLevelCodec :: TomlCodec LogLevel
logLevelCodec =
  Toml.dimap
    renderLogLevel
    parseLogLevel
    (Toml.text "level")

filePathCodec :: Toml.Key -> TomlCodec FilePath
filePathCodec key =
  Toml.dimap
    Text.pack
    Text.unpack
    (Toml.text key)

optionalWithDefault :: a -> TomlCodec a -> TomlCodec a
optionalWithDefault fallback =
  Toml.dimap
    Just
    (maybe fallback id)
    . Toml.dioptional

renderRoomId :: RoomId -> Text
renderRoomId (RoomId (EventId eventId)) = eventId

renderLogLevel :: LogLevel -> Text
renderLogLevel = \case
  DebugS -> "debug"
  InfoS -> "info"
  NoticeS -> "notice"
  WarningS -> "warning"
  ErrorS -> "error"
  CriticalS -> "critical"
  AlertS -> "alert"
  EmergencyS -> "emergency"

parseLogLevel :: Text -> LogLevel
parseLogLevel level =
  case Text.toLower level of
    "debug" -> DebugS
    "info" -> InfoS
    "notice" -> NoticeS
    "warn" -> WarningS
    "warning" -> WarningS
    "error" -> ErrorS
    "critical" -> CriticalS
    "alert" -> AlertS
    "emergency" -> EmergencyS
    _ -> InfoS
