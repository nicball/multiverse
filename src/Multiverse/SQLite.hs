module Multiverse.SQLite
  ( SQLiteTimeline
  , openTimeline
  , closeTimeline
  )
where

import Control.Exception (Exception, throwIO)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Database.SQLite.Simple
import Text.Read (readMaybe)
import Multiverse.Event
import Multiverse.Timeline
import Multiverse.Types

newtype SQLiteTimeline = SQLiteTimeline Connection

data StoredRow = StoredRow
  { rowSeq :: Int64
  , rowEventId :: Text
  , rowEvent :: Text
  }

instance FromRow StoredRow where
  fromRow = StoredRow <$> field <*> field <*> field

openTimeline :: FilePath -> IO SQLiteTimeline
openTimeline path = do
  connection <- open path
  migrate connection
  pure (SQLiteTimeline connection)

closeTimeline :: SQLiteTimeline -> IO ()
closeTimeline (SQLiteTimeline connection) = close connection

instance Timeline SQLiteTimeline where
  submit (SQLiteTimeline connection) = submitEvent connection
  getEvent (SQLiteTimeline connection) = lookupEvent connection
  getEventsAfter (SQLiteTimeline connection) = eventsAfter connection
  getUserInfo (SQLiteTimeline connection) = lookupUserInfo connection
  getRoomInfo (SQLiteTimeline connection) = lookupRoomInfo connection
  getMessage (SQLiteTimeline connection) = lookupMessage connection

migrate :: Connection -> IO ()
migrate connection = do
  execute_ connection "PRAGMA foreign_keys = ON"
  execute_
    connection
    "CREATE TABLE IF NOT EXISTS events\
    \ (seq INTEGER PRIMARY KEY AUTOINCREMENT,\
    \  event_id TEXT NOT NULL UNIQUE,\
    \  platform TEXT NOT NULL,\
    \  entity_type TEXT NOT NULL,\
    \  platform_key TEXT NOT NULL,\
    \  event TEXT NOT NULL,\
    \  UNIQUE (platform, entity_type, platform_key))"
  execute_
    connection
    "CREATE INDEX IF NOT EXISTS events_event_id_idx ON events (event_id)"

submitEvent :: Connection -> Event -> IO (Either SubmitError EventId)
submitEvent connection event = do
  let newId = eventId event
      newIdText = renderRead newId
  existing <- query connection "SELECT seq, event_id, event FROM events WHERE event_id = ?" (Only newIdText)
  case decodeStoredRows existing of
    Left err -> throwIO err
    Right (_ : _) -> pure (Right newId)
    Right [] -> do
      conflict <-
        query
          connection
          "SELECT event_id FROM events WHERE platform = ? AND entity_type = ? AND platform_key = ?"
          (event.platformKey.platform, event.platformKey.entityType, event.platformKey.key)
      case conflict of
        Only conflictingIdText : _ ->
          case parseRead conflictingIdText of
            Just conflictingId -> pure (Left (ConflictingPlatformKey event.platformKey conflictingId))
            Nothing -> throwIO (CorruptTimeline ("invalid event id: " <> conflictingIdText))
        [] -> do
          missing <- missingReferences connection event
          if null missing
            then do
              execute
                connection
                "INSERT INTO events (event_id, platform, entity_type, platform_key, event) VALUES (?, ?, ?, ?, ?)"
                ( newIdText
                , event.platformKey.platform
                , event.platformKey.entityType
                , event.platformKey.key
                , renderRead event
                )
              pure (Right newId)
            else pure (Left (MissingReferences missing))

lookupEvent :: Connection -> EventId -> IO (Maybe StoredEvent)
lookupEvent connection wantedId = do
  rows <- query connection "SELECT seq, event_id, event FROM events WHERE event_id = ?" (Only (renderRead wantedId))
  case decodeStoredRows rows of
    Left err -> throwIO err
    Right [] -> pure Nothing
    Right (stored : _) -> pure (Just stored)

eventsAfter :: Connection -> Maybe EventId -> IO [StoredEvent]
eventsAfter connection afterEventId = do
  rows <-
    case afterEventId of
      Nothing ->
        query_ connection "SELECT seq, event_id, event FROM events ORDER BY seq ASC"
      Just eventId_ ->
        query
          connection
          "SELECT seq, event_id, event FROM events\
          \ WHERE seq > (SELECT seq FROM events WHERE event_id = ?)\
          \ ORDER BY seq ASC"
          (Only (renderRead eventId_))
  either throwIO pure (decodeStoredRows rows)

lookupUserInfo :: Connection -> UserId -> Maybe RoomId -> IO (Maybe UserInfo)
lookupUserInfo connection user _room = do
  storedEvents <- eventsAfter connection Nothing
  pure (foldl' (applyUser user) Nothing storedEvents)

lookupRoomInfo :: Connection -> RoomId -> IO (Maybe RoomInfo)
lookupRoomInfo connection room = do
  storedEvents <- eventsAfter connection Nothing
  pure (foldl' (applyRoom room) Nothing storedEvents)

lookupMessage :: Connection -> MessageId -> IO (Maybe Message)
lookupMessage connection message = do
  storedEvents <- eventsAfter connection Nothing
  pure (foldl' (applyMessage message) Nothing storedEvents)

missingReferences :: Connection -> Event -> IO [EventId]
missingReferences connection event = filterMUnknown exists (eventReferences event)
 where
  exists eventId_ = do
    rows <- query connection "SELECT 1 FROM events WHERE event_id = ? LIMIT 1" (Only (renderRead eventId_)) :: IO [Only Int]
    pure (not (null rows))

filterMUnknown :: (a -> IO Bool) -> [a] -> IO [a]
filterMUnknown predicate = go []
 where
  go missing = \case
    [] -> pure (reverse missing)
    item : rest -> do
      ok <- predicate item
      if ok
        then go missing rest
        else go (item : missing) rest

decodeStoredRows :: [StoredRow] -> Either CorruptTimeline [StoredEvent]
decodeStoredRows = traverse decodeStoredRow

decodeStoredRow :: StoredRow -> Either CorruptTimeline StoredEvent
decodeStoredRow row =
  StoredEvent
    <$> parse "event id" row.rowEventId
    <*> parse "event" row.rowEvent

applyUser :: UserId -> Maybe UserInfo -> StoredEvent -> Maybe UserInfo
applyUser user current stored =
  case stored.storedEvent.content of
    CreateUser info
      | UserId stored.storedId == user -> Just info
    ModifyUser modifiedUser info
      | modifiedUser == user -> Just info
    _ -> current

applyRoom :: RoomId -> Maybe RoomInfo -> StoredEvent -> Maybe RoomInfo
applyRoom room current stored =
  case stored.storedEvent.content of
    CreateRoom info
      | RoomId stored.storedId == room -> Just info
    ModifyRoom modifiedRoom info
      | modifiedRoom == room -> Just info
    _ -> current

applyMessage :: MessageId -> Maybe Message -> StoredEvent -> Maybe Message
applyMessage message current stored =
  case stored.storedEvent.content of
    SendMessage info
      | MessageId stored.storedId == message -> Just info.body
    EditMessage _ editedMessage body
      | editedMessage == message -> Just body
    RetractMessage _ retractedMessage
      | retractedMessage == message -> Nothing
    _ -> current

renderRead :: Show a => a -> Text
renderRead = Text.pack . show

parseRead :: Read a => Text -> Maybe a
parseRead = readMaybe . Text.unpack

parse :: Read a => Text -> Text -> Either CorruptTimeline a
parse label value =
  maybe
    (Left (CorruptTimeline ("invalid " <> label <> ": " <> value)))
    Right
    (parseRead value)

newtype CorruptTimeline = CorruptTimeline Text
  deriving (Show)

instance Exception CorruptTimeline
