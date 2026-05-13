module Multiverse.Timeline
  ( Timeline (..)
  , StoredEvent (..)
  , SubmitError (..)
  )
where

import Multiverse.Event
import Multiverse.Types

class Timeline timeline where
  submit :: timeline -> Event -> IO (Either SubmitError EventId)
  getEvent :: timeline -> EventId -> IO (Maybe StoredEvent)
  getEventsAfter :: timeline -> Maybe TimelineSeq -> IO [StoredEvent]
  getUserInfo :: timeline -> UserId -> Maybe RoomId -> IO (Maybe UserInfo)
  getRoomInfo :: timeline -> RoomId -> IO (Maybe RoomInfo)
  getMessage :: timeline -> MessageId -> IO (Maybe Message)

data StoredEvent = StoredEvent
  { storedSeq :: TimelineSeq
  , storedId :: EventId
  , storedEvent :: Event
  }
  deriving (Eq, Show)

data SubmitError
  = ConflictingPlatformKey PlatformKey EventId
  | MissingReferences [EventId]
  deriving (Eq, Show)
