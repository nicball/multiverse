module Multiverse.Types
  ( EventId (..)
  , TimelineSeq (..)
  , UserId (..)
  , RoomId (..)
  , MessageId (..)
  , BlobId (..)
  , PlatformKey (..)
  , BlobType (..)
  , userEventId
  , roomEventId
  , messageEventId
  , blobEventId
  )
where

import Data.Text (Text)
import Data.Word (Word64)

newtype EventId = EventId Text
  deriving (Eq, Ord, Read, Show)

newtype TimelineSeq = TimelineSeq Word64
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

data BlobType = ImageBlob | VideoBlob | AudioBlob
  deriving (Eq, Ord, Read, Show)

userEventId :: UserId -> EventId
userEventId (UserId eventId) = eventId

roomEventId :: RoomId -> EventId
roomEventId (RoomId eventId) = eventId

messageEventId :: MessageId -> EventId
messageEventId (MessageId eventId) = eventId

blobEventId :: BlobId -> EventId
blobEventId (BlobId eventId) = eventId
