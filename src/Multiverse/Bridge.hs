module Multiverse.Bridge
  ( Bridge (..)
  , BridgeContext (..)
  , InitialRoomMapping (..)
  , MappingStore (..)
  , openMappingStore
  , ensureInitialRooms
  , ensureInitialRoomsWith
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Database.SQLite.Simple
import Text.Read (readMaybe)
import Multiverse.Event hiding (eventId)
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data Bridge timeline = Bridge
  { bridgeName :: Text
  , observe :: BridgeContext timeline -> IO ()
  , reflect :: BridgeContext timeline -> IO ()
  }

data BridgeContext timeline = BridgeContext
  { timeline :: timeline
  , mappingStore :: MappingStore
  , logger :: Logger
  }

data MappingStore = MappingStore
  { lookupTimelineId :: PlatformKey -> IO (Maybe EventId)
  , lookupPlatformKeys :: EventId -> IO [PlatformKey]
  , insertMapping :: PlatformKey -> EventId -> IO ()
  , lookupState :: Text -> IO (Maybe Text)
  , setState :: Text -> Text -> IO ()
  , closeMappingStore :: IO ()
  }

data InitialRoomMapping = InitialRoomMapping
  { platformKey :: PlatformKey
  , timelineRoom :: Maybe RoomId
  }
  deriving (Eq, Show)

openMappingStore :: FilePath -> IO MappingStore
openMappingStore path = do
  connection <- open path
  migrateMappingStore connection
  pure
    MappingStore
      { lookupTimelineId = lookupTimelineIdSql connection
      , lookupPlatformKeys = lookupPlatformKeysSql connection
      , insertMapping = insertMappingSql connection
      , lookupState = lookupStateSql connection
      , setState = setStateSql connection
      , closeMappingStore = close connection
      }

ensureInitialRooms :: Timeline timeline => [InitialRoomMapping] -> BridgeContext timeline -> IO ()
ensureInitialRooms mappings context =
  ensureInitialRoomsWith
    mappings
    context
    (\mapping -> pure RoomInfo {name = mapping.platformKey.key, description = "", avatar = Nothing})
    (\_ _ -> pure ())

ensureInitialRoomsWith ::
  Timeline timeline =>
  [InitialRoomMapping] ->
  BridgeContext timeline ->
  (InitialRoomMapping -> IO RoomInfo) ->
  (InitialRoomMapping -> RoomId -> IO ()) ->
  IO ()
ensureInitialRoomsWith mappings context fetchRoomInfo writeBack = do
  logInfo context.logger ("ensuring " <> Text.pack (show (length mappings)) <> " initial room mappings")
  mapM_ (ensureInitialRoom context fetchRoomInfo writeBack) mappings

ensureInitialRoom ::
  Timeline timeline =>
  BridgeContext timeline ->
  (InitialRoomMapping -> IO RoomInfo) ->
  (InitialRoomMapping -> RoomId -> IO ()) ->
  InitialRoomMapping ->
  IO ()
ensureInitialRoom context fetchRoomInfo writeBack mapping = do
  existing <- context.mappingStore.lookupTimelineId mapping.platformKey
  case existing of
    Just _ ->
      logDebug context.logger ("initial room already mapped " <> renderPlatformKey mapping.platformKey)
    Nothing ->
      case mapping.timelineRoom of
        Just room -> do
          context.mappingStore.insertMapping mapping.platformKey (roomEventId room)
          logInfo context.logger ("mapped initial room " <> renderPlatformKey mapping.platformKey)
        Nothing -> do
          roomInfo <- fetchRoomInfo mapping
          submitted <- submit context.timeline Event {platformKey = mapping.platformKey, content = CreateRoom roomInfo}
          case submitted of
            Right createdEventId -> do
              recordInitialRoom context writeBack mapping createdEventId
              logInfo context.logger ("created initial room " <> renderPlatformKey mapping.platformKey)
            Left (ConflictingPlatformKey _ existingEventId) -> do
              recordInitialRoom context writeBack mapping existingEventId
              logInfo context.logger ("recovered initial room mapping " <> renderPlatformKey mapping.platformKey)
            Left err -> ioError (userError ("could not create initial room mapping: " <> show err))

recordInitialRoom ::
  BridgeContext timeline ->
  (InitialRoomMapping -> RoomId -> IO ()) ->
  InitialRoomMapping ->
  EventId ->
  IO ()
recordInitialRoom context writeBack mapping mappedEventId = do
  let room = RoomId mappedEventId
  context.mappingStore.insertMapping mapping.platformKey mappedEventId
  writeBack mapping room

renderPlatformKey :: PlatformKey -> Text
renderPlatformKey platformKey =
  platformKey.platform <> ":" <> platformKey.entityType <> ":" <> platformKey.key

migrateMappingStore :: Connection -> IO ()
migrateMappingStore connection = do
  execute_
    connection
    "CREATE TABLE IF NOT EXISTS bridge_mappings\
    \ (platform TEXT NOT NULL,\
    \  entity_type TEXT NOT NULL,\
    \  platform_key TEXT NOT NULL,\
    \  event_id TEXT NOT NULL,\
    \  UNIQUE (platform, entity_type, platform_key),\
    \  UNIQUE (event_id, platform, entity_type, platform_key))"
  execute_
    connection
    "CREATE INDEX IF NOT EXISTS bridge_mappings_event_id_idx ON bridge_mappings (event_id)"
  execute_
    connection
    "CREATE TABLE IF NOT EXISTS bridge_state\
    \ (key TEXT PRIMARY KEY,\
    \  value TEXT NOT NULL)"

lookupTimelineIdSql :: Connection -> PlatformKey -> IO (Maybe EventId)
lookupTimelineIdSql connection platformKey = do
  rows <-
    query
      connection
      "SELECT event_id FROM bridge_mappings WHERE platform = ? AND entity_type = ? AND platform_key = ?"
      (platformKey.platform, platformKey.entityType, platformKey.key)
  case rows of
    Only eventIdText : _ ->
      case readEventId eventIdText of
        Just mappedEventId -> pure (Just mappedEventId)
        Nothing -> ioError (userError ("invalid mapped event id: " <> Text.unpack eventIdText))
    [] -> pure Nothing

lookupPlatformKeysSql :: Connection -> EventId -> IO [PlatformKey]
lookupPlatformKeysSql connection mappedEventId = do
  rows <-
    query
      connection
      "SELECT platform, entity_type, platform_key FROM bridge_mappings WHERE event_id = ? ORDER BY platform, entity_type, platform_key"
      (Only (renderEventId mappedEventId))
  pure (map toPlatformKey rows)

insertMappingSql :: Connection -> PlatformKey -> EventId -> IO ()
insertMappingSql connection platformKey mappedEventId =
  execute
    connection
    "INSERT OR IGNORE INTO bridge_mappings (platform, entity_type, platform_key, event_id) VALUES (?, ?, ?, ?)"
    (platformKey.platform, platformKey.entityType, platformKey.key, renderEventId mappedEventId)

lookupStateSql :: Connection -> Text -> IO (Maybe Text)
lookupStateSql connection key = do
  rows <- query connection "SELECT value FROM bridge_state WHERE key = ?" (Only key)
  pure case rows of
    Only value : _ -> Just value
    [] -> Nothing

setStateSql :: Connection -> Text -> Text -> IO ()
setStateSql connection key value =
  execute
    connection
    "INSERT INTO bridge_state (key, value) VALUES (?, ?)\
    \ ON CONFLICT(key) DO UPDATE SET value = excluded.value"
    (key, value)

toPlatformKey :: (Text, Text, Text) -> PlatformKey
toPlatformKey (platform, entityType, key) =
  PlatformKey {platform, entityType, key}

renderEventId :: EventId -> Text
renderEventId = Text.pack . show

readEventId :: Text -> Maybe EventId
readEventId = readMaybe . Text.unpack
