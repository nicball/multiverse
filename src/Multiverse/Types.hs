module Multiverse.Types
  ( EventId (..)
  , UserId (..)
  , RoomId (..)
  , MessageId (..)
  , BlobId (..)
  , PlatformKey (..)
  , BlobType (..)
  , renderEventId
  , persistedEventId
  , parsePersistedEventId
  , userEventId
  , roomEventId
  , messageEventId
  , blobEventId
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Text.Read (readMaybe)

newtype EventId = EventId Text
  deriving (Eq, Ord, Read, Show)

newtype UserId = UserId EventId
  deriving (Eq, Ord, Read, Show)

newtype RoomId = RoomId EventId
  deriving (Eq, Ord, Read, Show)

newtype MessageId = MessageId EventId
  deriving (Eq, Ord, Read, Show)

newtype BlobId = BlobId EventId
  deriving (Eq, Ord, Read, Show)

data PlatformKey = PlatformKey
  { platform :: Text
  , entityType :: Text
  , key :: Text
  }
  deriving (Eq, Ord, Read, Show)

data BlobType = ImageBlob | VideoBlob | AudioBlob | FileBlob
  deriving (Eq, Ord, Read, Show)

renderEventId :: EventId -> Text
renderEventId (EventId value) = value

-- | The sqlite stores persist event ids with the derived 'Show'/'Read'
-- instances, so existing databases stay readable. Everything else (platform
-- keys, URLs, configuration) uses the raw id from 'renderEventId'.
persistedEventId :: EventId -> Text
persistedEventId = Text.pack . show

parsePersistedEventId :: Text -> Maybe EventId
parsePersistedEventId = readMaybe . Text.unpack

userEventId :: UserId -> EventId
userEventId (UserId eventId) = eventId

roomEventId :: RoomId -> EventId
roomEventId (RoomId eventId) = eventId

messageEventId :: MessageId -> EventId
messageEventId (MessageId eventId) = eventId

blobEventId :: BlobId -> EventId
blobEventId (BlobId eventId) = eventId
