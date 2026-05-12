type EventId = Int
type UserId = EventId
type RoomId = EventId
type MessageId = EventId
type BlobId = EventId

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

data Timeline

sendEvent :: Timeline -> Event -> IO EventId
getEventsAfter :: Timeline -> EventId -> IO [(EventId, Event)]
getEvent :: Timeline -> EventId -> IO Event
getUserInfo :: Timeline -> UserId -> Maybe RoomId -> IO UserInfo
