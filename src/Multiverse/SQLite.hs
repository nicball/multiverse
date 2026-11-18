module Multiverse.SQLite
  ( SQLiteTimeline
  , openTimeline
  , closeTimeline
  )
where

import Control.Exception (Exception, throwIO)
import Control.Monad (filterM)
import Data.Text (Text)
import Data.Text qualified as Text
import Database.SQLite.Simple
import Text.Read (readMaybe)
import Multiverse.Event
import Multiverse.Timeline
import Multiverse.Types

newtype SQLiteTimeline = SQLiteTimeline Connection

data StoredRow = StoredRow
  { rowEventId :: Text
  , rowEvent :: Text
  }

instance FromRow StoredRow where
  fromRow = StoredRow <$> field <*> field

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
  getRoomMembers (SQLiteTimeline connection) = lookupRoomMembers connection
  getMembership (SQLiteTimeline connection) = lookupMembership connection
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
  existing <- eventExists connection newId
  if existing
    then pure (Right newId)
    else do
      conflict <- platformKeyConflict connection event.platformKey
      case conflict of
        Just conflictingId -> pure (Left (ConflictingPlatformKey event.platformKey conflictingId))
        Nothing -> do
          missing <- missingReferences connection event
          if null missing
            then do
              execute
                connection
                "INSERT OR IGNORE INTO events (event_id, platform, entity_type, platform_key, event) VALUES (?, ?, ?, ?, ?)"
                ( persistedEventId newId
                , event.platformKey.platform
                , event.platformKey.entityType
                , event.platformKey.key
                , renderEvent event
                )
              inserted <- eventExists connection newId
              if inserted
                then pure (Right newId)
                else do
                  raced <- platformKeyConflict connection event.platformKey
                  case raced of
                    Just conflictingId -> pure (Left (ConflictingPlatformKey event.platformKey conflictingId))
                    Nothing -> throwIO (CorruptTimeline ("event insert ignored: " <> persistedEventId newId))
            else pure (Left (MissingReferences missing))

eventExists :: Connection -> EventId -> IO Bool
eventExists connection wantedId = do
  rows <- query connection "SELECT 1 FROM events WHERE event_id = ? LIMIT 1" (Only (persistedEventId wantedId)) :: IO [Only Int]
  pure (not (null rows))

platformKeyConflict :: Connection -> PlatformKey -> IO (Maybe EventId)
platformKeyConflict connection platformKey = do
  rows <-
    query
      connection
      "SELECT event_id FROM events WHERE platform = ? AND entity_type = ? AND platform_key = ?"
      (platformKey.platform, platformKey.entityType, platformKey.key)
  case rows of
    Only conflictingIdText : _ ->
      case parsePersistedEventId conflictingIdText of
        Just conflictingId -> pure (Just conflictingId)
        Nothing -> throwIO (CorruptTimeline ("invalid event id: " <> conflictingIdText))
    [] -> pure Nothing

lookupEvent :: Connection -> EventId -> IO (Maybe StoredEvent)
lookupEvent connection wantedId = do
  rows <- query connection "SELECT event_id, event FROM events WHERE event_id = ?" (Only (persistedEventId wantedId))
  case decodeStoredRows rows of
    Left err -> throwIO err
    Right [] -> pure Nothing
    Right (stored : _) -> pure (Just stored)

eventsAfter :: Connection -> Maybe EventId -> IO [StoredEvent]
eventsAfter connection afterEventId = do
  rows <-
    case afterEventId of
      Nothing ->
        query_ connection "SELECT event_id, event FROM events ORDER BY seq ASC"
      Just eventId_ ->
        query
          connection
          "SELECT event_id, event FROM events\
          \ WHERE seq > (SELECT seq FROM events WHERE event_id = ?)\
          \ ORDER BY seq ASC"
          (Only (persistedEventId eventId_))
  either throwIO pure (decodeStoredRows rows)

-- | State queries replay the event log, so they all share this fold.
foldEvents :: Connection -> (a -> StoredEvent -> a) -> a -> IO a
foldEvents connection step initial = foldl' step initial <$> eventsAfter connection Nothing

lookupUserInfo :: Connection -> UserId -> Maybe RoomId -> IO (Maybe UserInfo)
lookupUserInfo connection user room =
  uncurry applyRoomName <$> foldEvents connection (applyUser user room) (Nothing, Nothing)
 where
  applyRoomName (Just value) (Just roomName) = Just UserInfo {name = roomName, avatar = value.avatar}
  applyRoomName info _ = info

lookupRoomMembers :: Connection -> RoomId -> IO [UserId]
lookupRoomMembers connection room =
  foldEvents connection (applyMembership room) []

-- | The head of a @(user, room)@ membership chain: the latest join or leave
-- event for that pair, whichever came last in submission order.
lookupMembership :: Connection -> UserId -> RoomId -> IO (Maybe EventId)
lookupMembership connection user room =
  foldEvents connection (applyMembershipEvent user room) Nothing

lookupRoomInfo :: Connection -> RoomId -> IO (Maybe RoomInfo)
lookupRoomInfo connection room =
  foldEvents connection (applyRoom room) Nothing

lookupMessage :: Connection -> MessageId -> IO (Maybe Message)
lookupMessage connection message =
  foldEvents connection (applyMessage message) Nothing

missingReferences :: Connection -> Event -> IO [EventId]
missingReferences connection event = filterM (fmap not . eventExists connection) (eventReferences event)

decodeStoredRows :: [StoredRow] -> Either CorruptTimeline [StoredEvent]
decodeStoredRows = traverse decodeStoredRow

decodeStoredRow :: StoredRow -> Either CorruptTimeline StoredEvent
decodeStoredRow row =
  StoredEvent
    <$> parseWith parsePersistedEventId "event id" row.rowEventId
    <*> parseWith parseEvent "event" row.rowEvent

applyUser :: UserId -> Maybe RoomId -> (Maybe UserInfo, Maybe Text) -> StoredEvent -> (Maybe UserInfo, Maybe Text)
applyUser user room (current, roomName) stored =
  case stored.storedEvent.content of
    CreateUser info
      | UserId stored.storedId == user -> (Just info, Nothing)
    ModifyUser modifiedUser info
      | modifiedUser == user -> (Just info, roomName)
    ChangeRoomNick changedUser changedRoom nick
      | changedUser == user, room == Just changedRoom -> (current, Just nick)
    _ -> (current, roomName)

applyMembershipEvent :: UserId -> RoomId -> Maybe EventId -> StoredEvent -> Maybe EventId
applyMembershipEvent user room current stored =
  case stored.storedEvent.content of
    JoinRoom joinedUser joinedRoom _
      | joinedUser == user, joinedRoom == room -> Just stored.storedId
    LeaveRoom leftUser leftRoom _
      | leftUser == user, leftRoom == room -> Just stored.storedId
    _ -> current

applyMembership :: RoomId -> [UserId] -> StoredEvent -> [UserId]
applyMembership room current stored =
  case stored.storedEvent.content of
    JoinRoom user joinedRoom _
      | joinedRoom == room -> if user `elem` current then current else current <> [user]
    LeaveRoom user leftRoom _
      | leftRoom == room -> filter (/= user) current
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
    EditMessage editedMessage body
      | editedMessage == message -> Just body
    RetractMessage retractedMessage
      | retractedMessage == message -> Nothing
    _ -> current

renderEvent :: Event -> Text
renderEvent = Text.pack . show

parseEvent :: Text -> Maybe Event
parseEvent = readMaybe . Text.unpack

parseWith :: (Text -> Maybe a) -> Text -> Text -> Either CorruptTimeline a
parseWith parser label value =
  maybe
    (Left (CorruptTimeline ("invalid " <> label <> ": " <> value)))
    Right
    (parser value)

newtype CorruptTimeline = CorruptTimeline Text
  deriving (Show)

instance Exception CorruptTimeline
