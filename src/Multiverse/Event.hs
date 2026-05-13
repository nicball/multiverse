module Multiverse.Event
  ( Event (..)
  , EventContent (..)
  , MessagePart (..)
  , InlineTextPart (..)
  , InlineText
  , Message
  , SendMessageInfo (..)
  , UserInfo (..)
  , RoomInfo (..)
  , eventId
  , eventReferences
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Crypto.Hash.SHA256 as SHA256
import Data.Char (intToDigit)
import Data.List.NonEmpty (NonEmpty, toList)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Word (Word8)
import Multiverse.Types

data Event = Event
  { platformKey :: PlatformKey
  , content :: EventContent
  }
  deriving (Eq, Ord, Read, Show)

data MessagePart
  = Text InlineText
  | Blob BlobType BlobId
  | Emote InlineText
  | List (NonEmpty Message)
  | BlockQuote Message
  deriving (Eq, Ord, Read, Show)

data InlineTextPart
  = Bold InlineText
  | Italic InlineText
  | Link InlineText Text
  | Mention InlineText UserId
  | InlineQuote InlineText
  | Plain Text
  deriving (Eq, Ord, Read, Show)

type InlineText = NonEmpty InlineTextPart

type Message = NonEmpty MessagePart

data SendMessageInfo = SendMessageInfo
  { sender :: UserId
  , room :: RoomId
  , replyTo :: Maybe MessageId
  , forwardOf :: Maybe MessageId
  , body :: Message
  }
  deriving (Eq, Ord, Read, Show)

data UserInfo = UserInfo
  { name :: Text
  , avatar :: Maybe BlobId
  }
  deriving (Eq, Ord, Read, Show)

data RoomInfo = RoomInfo
  { name :: Text
  , description :: Text
  , avatar :: Maybe BlobId
  }
  deriving (Eq, Ord, Read, Show)

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
  deriving (Eq, Ord, Read, Show)

eventId :: Event -> EventId
eventId event = EventId (sha256Hex (Text.pack (show event.platformKey) <> ":" <> showContent event.content))

eventReferences :: Event -> [EventId]
eventReferences event =
  case event.content of
    CreateRoom info -> maybe [] ((: []) . blobEventId) info.avatar
    CreateUser info -> maybe [] ((: []) . blobEventId) info.avatar
    CreateBlob _ -> []
    JoinRoom user room -> [userEventId user, roomEventId room]
    LeaveRoom user room -> [userEventId user, roomEventId room]
    SendMessage info ->
      userEventId info.sender
        : roomEventId info.room
        : foldMap ((: []) . messageEventId) info.replyTo
        <> foldMap ((: []) . messageEventId) info.forwardOf
        <> messageReferences info.body
    RetractMessage user message -> [userEventId user, messageEventId message]
    EditMessage user message body ->
      userEventId user : messageEventId message : messageReferences body
    ModifyUser user info ->
      userEventId user : maybe [] ((: []) . blobEventId) info.avatar
    ModifyRoom room info ->
      roomEventId room : maybe [] ((: []) . blobEventId) info.avatar
    ChangeRoomNick user room _ -> [userEventId user, roomEventId room]

messageReferences :: Message -> [EventId]
messageReferences = concatMap messagePartReferences . toList

messagePartReferences :: MessagePart -> [EventId]
messagePartReferences = \case
  Text inline -> inlineReferences inline
  Blob _ blob -> [blobEventId blob]
  Emote inline -> inlineReferences inline
  List messages -> concatMap messageReferences (toList messages)
  BlockQuote message -> messageReferences message

inlineReferences :: InlineText -> [EventId]
inlineReferences = concatMap inlinePartReferences . toList

inlinePartReferences :: InlineTextPart -> [EventId]
inlinePartReferences = \case
  Bold inline -> inlineReferences inline
  Italic inline -> inlineReferences inline
  Link inline _ -> inlineReferences inline
  Mention inline user -> userEventId user : inlineReferences inline
  InlineQuote inline -> inlineReferences inline
  Plain _ -> []

showContent :: EventContent -> Text
showContent = \case
  CreateBlob bytes -> "CreateBlob:" <> bytesText bytes
  other -> Text.pack (show other)

bytesText :: ByteString -> Text
bytesText = Text.pack . show . ByteString.unpack

sha256Hex :: Text -> Text
sha256Hex =
  Text.pack
    . concatMap byteHex
    . ByteString.unpack
    . SHA256.hash
    . Text.encodeUtf8

byteHex :: Word8 -> String
byteHex byte =
  [ intToDigit (fromIntegral byte `div` 16)
  , intToDigit (fromIntegral byte `mod` 16)
  ]
