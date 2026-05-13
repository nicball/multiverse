module Agent where

import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty)
import Data.Text (Text)

newtype EventId = EventId Text
newtype UserId = UserId EventId
newtype RoomId = RoomId EventId
newtype MessageId = MessageId EventId
newtype BlobId = BlobId EventId

data PlatformKey = PlatformKey
  { platform :: Text
  , entityType :: Text
  , key :: Text
  }

data Event = Event
  { platformKey :: PlatformKey
  , content :: EventContent
  }

data MessagePart
  = Text InlineText
  | Blob BlobType BlobId
  | Emote InlineText
  | List (NonEmpty Message)
  | BlockQuote Message

data InlineTextPart
  = Bold InlineText
  | Italic InlineText
  | Link InlineText Text
  | Mention InlineText UserId
  | InlineQuote InlineText
  | Plain Text

type InlineText = NonEmpty InlineTextPart

data BlobType = ImageBlob | VideoBlob | AudioBlob

type Message = NonEmpty MessagePart

data SendMessageInfo = SendMessageInfo
  { sender :: UserId
  , room :: RoomId
  , replyTo :: Maybe MessageId
  , forwardOf :: Maybe MessageId
  , body :: Message
  }

data UserInfo = UserInfo
  { name :: Text
  , avatar :: Maybe BlobId
  }

data RoomInfo = RoomInfo
  { name :: Text
  , description :: Text
  , avatar :: Maybe BlobId
  }

data EventContent
  = CreateRoom RoomInfo
  | CreateUser UserInfo
  | CreateBlob ByteString
  | JoinRoom UserId RoomId
  | LeaveRoom UserId RoomId
  | SendMessage SendMessageInfo
  | RetractMessage UserId MessageId
  | EditMessage UserId MessageId Message
  | ModifyUser UserId UserInfo
  | ModifyRoom RoomId RoomInfo
  | ChangeRoomNick UserId RoomId Text

class Timeline timeline where
  submit :: timeline -> Event -> IO (Either SubmitError EventId)
  getEvent :: timeline -> EventId -> IO (Maybe StoredEvent)
  getEventsAfter :: timeline -> Maybe EventId -> IO [StoredEvent]
  getUserInfo :: timeline -> UserId -> Maybe RoomId -> IO (Maybe UserInfo)
  getRoomInfo :: timeline -> RoomId -> IO (Maybe RoomInfo)
  getMessage :: timeline -> MessageId -> IO (Maybe Message)

data StoredEvent = StoredEvent
  { storedId :: EventId
  , storedEvent :: Event
  }

data SubmitError
  = ConflictingPlatformKey PlatformKey EventId
  | MissingReferences [EventId]

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
  }

data InitialRoomMapping = InitialRoomMapping
  { platformKey :: PlatformKey
  , timelineRoom :: Maybe RoomId
  }

data Logger

-- Bridge room config is an allow list. Observers ignore platform rooms not
-- listed in config. If an initial room lacks a timelineRoom, the bridge creates
-- a timeline room and writes the generated id back to TOML config.
ensureInitialRooms ::
  Timeline timeline =>
  [InitialRoomMapping] ->
  BridgeContext timeline ->
  IO ()
ensureInitialRooms = undefined

-- Observers use this path for idempotent platform event/entity submission:
-- submit to timeline, accept existing events on platform-key conflicts, and
-- persist the bridge mapping.
submitMapped ::
  Timeline timeline =>
  BridgeContext timeline ->
  Event ->
  IO (Either SubmitError EventId)
submitMapped = undefined

-- Reflectors must skip events whose origin platform is the same as the bridge,
-- even when the platform user is not known in the timeline yet.
shouldReflectToPlatform :: Text -> StoredEvent -> Bool
shouldReflectToPlatform bridgePlatform stored =
  platform (platformKey (storedEvent stored)) /= bridgePlatform

-- HTTP requests made by bridges are logged at debug level with credentials
-- redacted and retried on network exceptions with exponential backoff.
data HttpLogOptions = HttpLogOptions
  { redactUrl :: Text -> Text
  , requestSuffix :: Text
  }
