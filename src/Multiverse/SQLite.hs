module Multiverse.SQLite
  ( SQLiteTimeline
  , openTimeline
  , closeTimeline
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Exception (Exception, throwIO)
import Control.Monad (filterM)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Database.SQLite.Simple
import Text.Read (readMaybe)
import Multiverse.Event
import Multiverse.Timeline
import Multiverse.Types

-- | The connection is guarded by an 'MVar': every bridge observer and
-- reflector queries the same timeline concurrently, and one sqlite connection
-- (and the statement state behind it) cannot be shared between threads.
newtype SQLiteTimeline = SQLiteTimeline (MVar Connection)

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
  SQLiteTimeline <$> newMVar connection

closeTimeline :: SQLiteTimeline -> IO ()
closeTimeline timeline = withTimelineDb timeline close

withTimelineDb :: SQLiteTimeline -> (Connection -> IO a) -> IO a
withTimelineDb (SQLiteTimeline connection) action = withMVar connection action

instance Timeline SQLiteTimeline where
  submit timeline event = withTimelineDb timeline \connection -> submitEvent connection event
  getEvent timeline event = withTimelineDb timeline \connection -> lookupEvent connection event
  getEventsAfter timeline afterEventId = withTimelineDb timeline \connection -> eventsAfter connection afterEventId
  getUserInfo timeline user room = withTimelineDb timeline \connection -> lookupUserInfo connection user room
  getUserInfoAt timeline user room upper = withTimelineDb timeline \connection -> lookupUserInfoAt connection user room upper
  getRoomUserInfo timeline user room = withTimelineDb timeline \connection -> lookupRoomUserInfo connection user room
  getRoomInfo timeline room = withTimelineDb timeline \connection -> lookupRoomInfo connection room
  getUserProfileEvent timeline user = withTimelineDb timeline \connection -> lookupUserProfileEvent connection user
  getRoomProfileEvent timeline room = withTimelineDb timeline \connection -> lookupRoomProfileEvent connection room
  getRoomMembers timeline room = withTimelineDb timeline \connection -> lookupRoomMembers connection room
  getMembership timeline user room = withTimelineDb timeline \connection -> lookupMembership connection user room
  getMessageState timeline message = withTimelineDb timeline \connection -> lookupMessageState connection message
  getMessageRoot timeline event = withTimelineDb timeline \connection -> lookupMessageRoot connection event

migrate :: Connection -> IO ()
migrate connection = do
  -- WAL lets another process (a sqlite shell, a backup) read while bridges
  -- write, and a busy timeout turns a momentary lock into a short wait instead
  -- of an immediate 'SQLITE_BUSY'.
  execute_ connection "PRAGMA journal_mode = WAL"
  execute_ connection "PRAGMA busy_timeout = 5000"
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

-- | The checks and the insert form one transaction: two bridges can submit
-- concurrently, and the loser must observe the winner's event id instead of a
-- constraint violation.
submitEvent :: Connection -> Event -> IO (Either SubmitError EventId)
submitEvent connection event =
  withImmediateTransaction connection (insertEvent connection event)

insertEvent :: Connection -> Event -> IO (Either SubmitError EventId)
insertEvent connection event = do
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

eventsThrough :: Connection -> EventId -> IO [StoredEvent]
eventsThrough connection upperEventId = do
  rows <-
    query
      connection
      "SELECT event_id, event FROM events\
      \ WHERE seq <= (SELECT seq FROM events WHERE event_id = ?)\
      \ ORDER BY seq ASC"
      (Only (persistedEventId upperEventId))
  either throwIO pure (decodeStoredRows rows)

-- | State queries replay the event log, so they all share this fold.
foldEvents :: Connection -> (a -> StoredEvent -> a) -> a -> IO a
foldEvents connection step initial = foldl' step initial <$> eventsAfter connection Nothing

data UserView = UserView
  { globalInfo :: Maybe UserInfo
  , roomInfo :: Maybe UserInfo
  }

emptyUserView :: UserView
emptyUserView = UserView Nothing Nothing

effectiveUserInfo :: UserView -> Maybe UserInfo
effectiveUserInfo view = view.roomInfo <|> view.globalInfo

lookupUserInfo :: Connection -> UserId -> Maybe RoomId -> IO (Maybe UserInfo)
lookupUserInfo connection user room =
  effectiveUserInfo <$> foldEvents connection (applyUser user room) emptyUserView

lookupUserInfoAt :: Connection -> UserId -> Maybe RoomId -> EventId -> IO (Maybe UserInfo)
lookupUserInfoAt connection user room upper =
  effectiveUserInfo <$> foldEventsThrough connection upper (applyUser user room) emptyUserView

lookupRoomUserInfo :: Connection -> UserId -> RoomId -> IO (Maybe UserInfo)
lookupRoomUserInfo connection user room =
  fmap (\view -> view.roomInfo) (foldEvents connection (applyUser user (Just room)) emptyUserView)

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

-- | The latest profile-state event for a user: the creation event until the
-- first modification, then the latest global or room-scoped modification.
lookupUserProfileEvent :: Connection -> UserId -> IO (Maybe EventId)
lookupUserProfileEvent connection user =
  foldEvents connection (applyUserProfileEvent user) Nothing

lookupRoomProfileEvent :: Connection -> RoomId -> IO (Maybe EventId)
lookupRoomProfileEvent connection room =
  foldEvents connection (applyRoomProfileEvent room) Nothing

lookupMessageState :: Connection -> MessageId -> IO (Maybe MessageState)
lookupMessageState connection message = do
  stored <- eventsAfter connection Nothing
  let msgFold = foldMessages stored
  pure (Map.lookup message msgFold.messageStates)

lookupMessageRoot :: Connection -> EventId -> IO (Maybe MessageId)
lookupMessageRoot connection headEventId = do
  stored <- eventsAfter connection Nothing
  let msgFold = foldMessages stored
  pure (Map.lookup headEventId msgFold.messageRoots)

missingReferences :: Connection -> Event -> IO [EventId]
missingReferences connection event = filterM (fmap not . eventExists connection) (eventReferences event)

decodeStoredRows :: [StoredRow] -> Either CorruptTimeline [StoredEvent]
decodeStoredRows = traverse decodeStoredRow

decodeStoredRow :: StoredRow -> Either CorruptTimeline StoredEvent
decodeStoredRow row =
  StoredEvent
    <$> parseWith parsePersistedEventId "event id" row.rowEventId
    <*> parseWith parseEvent "event" row.rowEvent

foldEventsThrough :: Connection -> EventId -> (a -> StoredEvent -> a) -> a -> IO a
foldEventsThrough connection upper step initial =
  foldl' step initial <$> eventsThrough connection upper

applyUser :: UserId -> Maybe RoomId -> UserView -> StoredEvent -> UserView
applyUser user room view stored =
  case stored.storedEvent.content of
    CreateUser info
      | UserId stored.storedId == user -> view {globalInfo = Just info}
    ModifyUser modifiedUser info _
      | modifiedUser == user -> view {globalInfo = Just info}
    ModifyUserPerRoom modifiedUser modifiedRoom info _
      | modifiedUser == user, room == Just modifiedRoom -> view {roomInfo = info}
    _ -> view

applyUserProfileEvent :: UserId -> Maybe EventId -> StoredEvent -> Maybe EventId
applyUserProfileEvent user current stored =
  case stored.storedEvent.content of
    CreateUser _
      | UserId stored.storedId == user -> Just stored.storedId
    ModifyUser modifiedUser _ _
      | modifiedUser == user -> Just stored.storedId
    ModifyUserPerRoom modifiedUser _ _ _
      | modifiedUser == user -> Just stored.storedId
    _ -> current

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
    ModifyRoom modifiedRoom info _
      | modifiedRoom == room -> Just info
    _ -> current

applyRoomProfileEvent :: RoomId -> Maybe EventId -> StoredEvent -> Maybe EventId
applyRoomProfileEvent room current stored =
  case stored.storedEvent.content of
    CreateRoom _
      | RoomId stored.storedId == room -> Just stored.storedId
    ModifyRoom modifiedRoom _ _
      | modifiedRoom == room -> Just stored.storedId
    _ -> current

data MessageFold = MessageFold
  { messageRoots :: Map.Map EventId MessageId
  , messageStates :: Map.Map MessageId MessageState
  }

emptyMessageFold :: MessageFold
emptyMessageFold = MessageFold Map.empty Map.empty

foldMessages :: [StoredEvent] -> MessageFold
foldMessages = foldl' step emptyMessageFold
 where
  step msgFold stored =
    case stored.storedEvent.content of
      SendMessage info ->
        let root = MessageId stored.storedId
         in msgFold
              { messageRoots = Map.insert stored.storedId root msgFold.messageRoots
              , messageStates = Map.insert root (MessageState stored.storedId (Just info.body)) msgFold.messageStates
              }
      EditMessage previous body ->
        case Map.lookup previous msgFold.messageRoots of
          Nothing -> msgFold
          Just root ->
            let currentBody = Map.lookup root msgFold.messageStates >>= (.messageBody)
                nextBody = body <$ currentBody
             in msgFold
                  { messageRoots = Map.insert stored.storedId root msgFold.messageRoots
                  , messageStates = Map.insert root (MessageState stored.storedId nextBody) msgFold.messageStates
                  }
      RetractMessage previous ->
        case Map.lookup previous msgFold.messageRoots of
          Nothing -> msgFold
          Just root ->
            msgFold
              { messageRoots = Map.insert stored.storedId root msgFold.messageRoots
              , messageStates = Map.insert root (MessageState stored.storedId Nothing) msgFold.messageStates
              }
      _ -> msgFold

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
