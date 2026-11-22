module Multiverse.Timeline
  ( Timeline (..)
  , StoredEvent (..)
  , SubmitError (..)
  , MessageState (..)
  )
where

import Multiverse.Event
import Multiverse.Types

class Timeline timeline where
  submit :: timeline -> Event -> IO (Either SubmitError EventId)
  getEvent :: timeline -> EventId -> IO (Maybe StoredEvent)
  getEventsAfter :: timeline -> Maybe EventId -> IO [StoredEvent]
  getUserInfo :: timeline -> UserId -> Maybe RoomId -> IO (Maybe UserInfo)
  getUserInfoAt :: timeline -> UserId -> Maybe RoomId -> EventId -> IO (Maybe UserInfo)
  getRoomUserInfo :: timeline -> UserId -> RoomId -> IO (Maybe UserInfo)
  getRoomInfo :: timeline -> RoomId -> IO (Maybe RoomInfo)
  getUserProfileEvent :: timeline -> UserId -> IO (Maybe EventId)
  getRoomProfileEvent :: timeline -> RoomId -> IO (Maybe EventId)
  getRoomMembers :: timeline -> RoomId -> IO [UserId]
  getMembership :: timeline -> UserId -> RoomId -> IO (Maybe EventId)
  getMessageState :: timeline -> MessageId -> IO (Maybe MessageState)
  getMessageRoot :: timeline -> EventId -> IO (Maybe MessageId)

data StoredEvent = StoredEvent
  { storedId :: EventId
  , storedEvent :: Event
  }
  deriving (Eq, Show)

-- | The current state of a message chain. 'messageBody' is 'Nothing' once the
-- message has been retracted.
data MessageState = MessageState
  { messageHead :: EventId
  , messageBody :: Maybe Message
  }
  deriving (Eq, Show)

data SubmitError
  = ConflictingPlatformKey PlatformKey EventId
  | MissingReferences [EventId]
  deriving (Eq, Show)
