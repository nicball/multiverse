module Multiverse.Event
  ( Event (..)
  , EventContent (..)
  , MessagePart (..)
  , InlineTextPart (..)
  , InlineText
  , Message
  , plainInline
  , plainMessage
  , inlineToText
  , messageMentionTargets
  , SendMessageInfo (..)
  , UserInfo (..)
  , RoomInfo (..)
  , eventId
  , eventReferences
  , blobPlatformKey
  , contentDigest
  , contentDigestBytes
  )
where

import Crypto.Hash.SHA256 qualified as SHA256
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.Char (intToDigit)
import Data.List.NonEmpty (NonEmpty ((:|)), toList)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
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
  | Heading Int InlineText
  deriving (Eq, Ord, Read, Show)

data InlineTextPart
  = Bold InlineText
  | Italic InlineText
  | Link InlineText Text
  -- The target is the event the mention refers to, so a bridge interprets it
  -- by the target's content: a user creation event is a user mention, a message
  -- is a message link, and a room creation event can stand for @all when it is
  -- the room the message is being reflected into.
  | Mention InlineText EventId
  | InlineQuote InlineText
  | Plain Text
  deriving (Eq, Ord, Read, Show)

type InlineText = NonEmpty InlineTextPart

type Message = NonEmpty MessagePart

plainInline :: Text -> InlineText
plainInline text = Plain text :| []

plainMessage :: Text -> Message
plainMessage text = Text (plainInline text) :| []

inlineToText :: InlineText -> Text
inlineToText = Text.concat . map partText . toList
 where
  partText = \case
    Plain text -> text
    Bold inline -> inlineToText inline
    Italic inline -> inlineToText inline
    Link inline _ -> inlineToText inline
    Mention inline _ -> inlineToText inline
    InlineQuote inline -> inlineToText inline

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
  | JoinRoom UserId RoomId (Maybe EventId)
  | LeaveRoom UserId RoomId (Maybe EventId)
  | SendMessage SendMessageInfo
  -- | The previous state event in the message chain. The chain root is the
  -- message identity.
  | RetractMessage EventId
  | EditMessage EventId Message
  | ModifyUser UserId UserInfo EventId
  | ModifyRoom RoomId RoomInfo EventId
  -- | A complete room-scoped profile. 'Nothing' clears the override so the
  -- room falls back to the global profile.
  | ModifyUserPerRoom UserId RoomId (Maybe UserInfo) EventId
  deriving (Eq, Ord, Read, Show)

eventId :: Event -> EventId
eventId event = EventId (sha256Hex (Text.pack (show event.platformKey) <> ":" <> showContent event.content))

eventReferences :: Event -> [EventId]
eventReferences event =
  case event.content of
    CreateRoom info -> maybe [] ((: []) . blobEventId) info.avatar
    CreateUser info -> maybe [] ((: []) . blobEventId) info.avatar
    CreateBlob _ -> []
    JoinRoom user room previous ->
      [userEventId user, roomEventId room] <> maybe [] pure previous
    LeaveRoom user room previous ->
      [userEventId user, roomEventId room] <> maybe [] pure previous
    SendMessage info ->
      userEventId info.sender
        : roomEventId info.room
        : foldMap ((: []) . messageEventId) info.replyTo
        <> foldMap ((: []) . messageEventId) info.forwardOf
        <> messageReferences info.body
    RetractMessage previous -> [previous]
    EditMessage previous body ->
      previous : messageReferences body
    ModifyUser user info previous ->
      userEventId user : (maybe [] ((: []) . blobEventId) info.avatar <> [previous])
    ModifyRoom room info previous ->
      roomEventId room : (maybe [] ((: []) . blobEventId) info.avatar <> [previous])
    ModifyUserPerRoom user room info previous ->
      userEventId user
        : roomEventId room
        : (maybe [] ((: []) . blobEventId) (info >>= \value -> value.avatar) <> [previous])

messageReferences :: Message -> [EventId]
messageReferences = concatMap messagePartReferences . toList

messagePartReferences :: MessagePart -> [EventId]
messagePartReferences = \case
  Text inline -> inlineReferences inline
  Blob _ blob -> [blobEventId blob]
  Emote inline -> inlineReferences inline
  List messages -> concatMap messageReferences (toList messages)
  BlockQuote message -> messageReferences message
  Heading _ inline -> inlineReferences inline

inlineReferences :: InlineText -> [EventId]
inlineReferences = concatMap inlinePartReferences . toList

inlinePartReferences :: InlineTextPart -> [EventId]
inlinePartReferences = \case
  Bold inline -> inlineReferences inline
  Italic inline -> inlineReferences inline
  Link inline _ -> inlineReferences inline
  Mention inline target -> target : inlineReferences inline
  InlineQuote inline -> inlineReferences inline
  Plain _ -> []

-- | The events mentioned by a message, in message order. Reflectors resolve
-- each distinct target once instead of once per occurrence.
messageMentionTargets :: Message -> [EventId]
messageMentionTargets = concatMap partTargets . toList
 where
  partTargets = \case
    Text inline -> inlineTargets inline
    Emote inline -> inlineTargets inline
    Blob _ _ -> []
    List messages -> concatMap messageMentionTargets (toList messages)
    BlockQuote message -> messageMentionTargets message
    Heading _ inline -> inlineTargets inline

inlineTargets :: InlineText -> [EventId]
inlineTargets = concatMap partTargets . toList
 where
  partTargets = \case
    Bold inline -> inlineTargets inline
    Italic inline -> inlineTargets inline
    Link inline _ -> inlineTargets inline
    Mention _ target -> [target]
    InlineQuote inline -> inlineTargets inline
    Plain _ -> []

blobPlatformKey :: ByteString -> PlatformKey
blobPlatformKey bytes = PlatformKey "multiverse" "blob" (sha256HexBytes bytes)

-- | A stable digest for values that only have content identity, such as
-- synthetic profile-modification platform keys.
contentDigest :: Text -> Text
contentDigest = sha256Hex

contentDigestBytes :: ByteString -> Text
contentDigestBytes = sha256HexBytes

showContent :: EventContent -> Text
showContent = \case
  CreateBlob bytes -> "CreateBlob:" <> sha256HexBytes bytes
  other -> Text.pack (show other)

sha256Hex :: Text -> Text
sha256Hex = sha256HexBytes . Text.Encoding.encodeUtf8

sha256HexBytes :: ByteString -> Text
sha256HexBytes =
  Text.pack
    . concatMap byteHex
    . ByteString.unpack
    . SHA256.hash

byteHex :: Word8 -> String
byteHex byte =
  [ intToDigit (fromIntegral byte `div` 16)
  , intToDigit (fromIntegral byte `mod` 16)
  ]
