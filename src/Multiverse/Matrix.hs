module Multiverse.Matrix
  ( MatrixConfig (..)
  , matrixBridge
  )
where

import Control.Concurrent (threadDelay)
import Control.Monad (forever, unless)
import Data.Aeson
import Data.Aeson.Types (Parser)
import Data.Aeson.Key qualified as Aeson.Key
import Data.Aeson.KeyMap qualified as Aeson.KeyMap
import Data.ByteString.Lazy qualified as LByteString
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types.URI (urlEncode)
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Event hiding (eventId)
import Multiverse.HTTP (HttpLogOptions (..), defaultHttpLogOptions)
import Multiverse.HTTP qualified as HTTP
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data MatrixConfig = MatrixConfig
  { homeserver :: Text
  , accessToken :: Text
  , initialRooms :: [InitialRoomMapping]
  , syncTimeoutMs :: Int
  , noteInitialRoom :: InitialRoomMapping -> RoomId -> IO ()
  }

matrixBridge :: Timeline timeline => MatrixConfig -> Bridge timeline
matrixBridge config =
  Bridge
    { bridgeName = "matrix"
    , observe = observeMatrix config
    , reflect = reflectMatrix config
    }

observeMatrix :: Timeline timeline => MatrixConfig -> BridgeContext timeline -> IO ()
observeMatrix config context = do
  manager <- newManager tlsManagerSettings
  whoami <- matrixWhoami context.logger manager config
  ownUserId <-
    case whoami of
      Right value -> do
        logInfo context.logger ("matrix bridge user id " <> value.userId)
        pure value.userId
      Left err -> ioError (userError ("matrix whoami failed: " <> err))
  ensureInitialRoomsWith
    config.initialRooms
    context
    (fetchInitialRoomInfo context.logger manager config)
    config.noteInitialRoom
  forever do
    since <- context.mappingStore.lookupState "matrix.next_batch"
    response <- matrixSync context.logger manager config since
    case response of
      Left err -> do
        logError context.logger ("matrix observe error: " <> Text.pack err)
        threadDelay 5000000
      Right sync -> do
        mapM_ (observeRoomEvents manager config context ownUserId) sync.rooms
        context.mappingStore.setState "matrix.next_batch" sync.nextBatch

reflectMatrix :: Timeline timeline => MatrixConfig -> BridgeContext timeline -> IO ()
reflectMatrix config context = do
  manager <- newManager tlsManagerSettings
  forever do
    afterSeq <- fmap (>>= readTimelineSeq) (context.mappingStore.lookupState "matrix.reflect_seq")
    storedEvents <- getEventsAfter context.timeline afterSeq
    mapM_ (reflectEvent manager config context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "matrix.reflect_seq" (Text.pack (show (maximum (map (.storedSeq) storedEvents))))

observeRoomEvents :: Timeline timeline => Manager -> MatrixConfig -> BridgeContext timeline -> Text -> MatrixRoomEvents -> IO ()
observeRoomEvents manager config context ownUserId roomEvents = do
  room <- configuredRoom config context roomEvents.roomId
  case room of
    Nothing -> logDebug context.logger ("matrix ignoring unconfigured room " <> roomEvents.roomId)
    Just roomId -> mapM_ (observeEvent manager context config ownUserId roomId) roomEvents.events

observeEvent :: Timeline timeline => Manager -> BridgeContext timeline -> MatrixConfig -> Text -> RoomId -> MatrixEvent -> IO ()
observeEvent manager context config ownUserId room event
  | event.sender == ownUserId =
      logDebug context.logger ("matrix ignoring own event " <> event.eventId)
  | otherwise =
      case event.body of
        Nothing -> pure ()
        Just body -> do
          userId <- ensureUser manager context config event.sender
          let platformKey = matrixEventKey event.eventId
              timelineEvent =
                Event
                  { platformKey
                  , content =
                      SendMessage
                        SendMessageInfo
                          { sender = userId
                          , room
                          , replyTo = Nothing
                          , forwardOf = Nothing
                          , body = plainMessage body
                          }
                  }
          submitted <- submitMapped context timelineEvent
          case submitted of
            Right _ -> pure ()
            Left err -> logError context.logger ("matrix submit error: " <> Text.pack (show err))

ensureUser :: Timeline timeline => Manager -> BridgeContext timeline -> MatrixConfig -> Text -> IO UserId
ensureUser manager context config matrixUserId = do
  let platformKey = matrixUserKey matrixUserId
  existing <- context.mappingStore.lookupTimelineId platformKey
  case existing of
    Just eventId -> pure (UserId eventId)
    Nothing -> do
      displayName <- matrixUserDisplayName context.logger manager config matrixUserId
      let event = Event {platformKey, content = CreateUser UserInfo {name = maybe matrixUserId id displayName, avatar = Nothing}}
      mapped <- submitMapped context event
      case mapped of
        Right eventId -> pure (UserId eventId)
        Left err -> fail ("could not create matrix user: " <> show err)

configuredRoom :: MatrixConfig -> BridgeContext timeline -> Text -> IO (Maybe RoomId)
configuredRoom config context matrixRoomId = do
  let platformKey = matrixRoomKey matrixRoomId
  if platformKey `elem` map (.platformKey) config.initialRooms
    then fmap RoomId <$> context.mappingStore.lookupTimelineId platformKey
    else pure Nothing

reflectEvent :: Timeline timeline => Manager -> MatrixConfig -> BridgeContext timeline -> StoredEvent -> IO ()
reflectEvent manager config context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          alreadyMapped <- hasPlatformMapping context.mappingStore "matrix" stored.storedId
          unless alreadyMapped do
            roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
            let matrixRooms = filter ((== "matrix") . (.platform)) roomKeys
            mapM_ (sendToRoom info) matrixRooms
    _ -> pure ()
 where
  sendToRoom info roomKey = do
    text <- renderRelayedMessage context info
    result <- matrixSendMessage context.logger manager config roomKey.key stored.storedId text
    case result of
      Left err -> logError context.logger ("matrix reflect error: " <> Text.pack err)
      Right matrixEventId -> context.mappingStore.insertMapping (matrixEventKey matrixEventId) stored.storedId

matrixSync :: Logger -> Manager -> MatrixConfig -> Maybe Text -> IO (Either String MatrixSync)
matrixSync logger manager config since = do
  request0 <- authorizedRequest config (config.homeserver <> "/_matrix/client/v3/sync")
  let query =
        [ ("timeout", Just (Text.encodeUtf8 (Text.pack (show config.syncTimeoutMs))))
        ]
          <> maybe [] (\token -> [("since", Just (Text.encodeUtf8 token))]) since
      request = setQueryString query request0
  response <- loggedHttp logger request manager
  pure (decodeMatrix response)

matrixWhoami :: Logger -> Manager -> MatrixConfig -> IO (Either String MatrixWhoami)
matrixWhoami logger manager config = do
  request <- authorizedRequest config (config.homeserver <> "/_matrix/client/v3/account/whoami")
  response <- loggedHttp logger request manager
  pure (decodeMatrix response)

matrixUserDisplayName :: Logger -> Manager -> MatrixConfig -> Text -> IO (Maybe Text)
matrixUserDisplayName logger manager config matrixUserId = do
  result <- matrixGetDisplayName logger manager config matrixUserId
  case result of
    Right value -> pure value.displayName
    Left err -> do
      logDebug logger ("matrix display name lookup failed for " <> matrixUserId <> ": " <> Text.pack err)
      pure Nothing

matrixGetDisplayName :: Logger -> Manager -> MatrixConfig -> Text -> IO (Either String MatrixDisplayName)
matrixGetDisplayName logger manager config matrixUserId = do
  request <- authorizedRequest config (config.homeserver <> "/_matrix/client/v3/profile/" <> escapePathSegment matrixUserId <> "/displayname")
  response <- loggedHttp logger request manager
  pure (decodeMatrix response)

matrixSendMessage :: Logger -> Manager -> MatrixConfig -> Text -> EventId -> Text -> IO (Either String Text)
matrixSendMessage logger manager config roomId eventId_ body = do
  request0 <-
    authorizedRequest
      config
      ( config.homeserver
          <> "/_matrix/client/v3/rooms/"
          <> roomId
          <> "/send/m.room.message/"
          <> transactionId eventId_
      )
  let request =
        request0
          { method = "PUT"
          , requestBody = RequestBodyLBS (encode (object ["msgtype" .= ("m.text" :: Text), "body" .= body]))
          , requestHeaders = ("Content-Type", "application/json") : request0.requestHeaders
          }
  response <- loggedHttp logger request manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

fetchInitialRoomInfo :: Logger -> Manager -> MatrixConfig -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo logger manager config mapping = do
  name <- matrixRoomName logger manager config mapping.platformKey.key
  topic <- matrixRoomTopic logger manager config mapping.platformKey.key
  pure
    RoomInfo
      { name = maybe mapping.platformKey.key id name
      , description = maybe "" id topic
      , avatar = Nothing
      }

matrixRoomName :: Logger -> Manager -> MatrixConfig -> Text -> IO (Maybe Text)
matrixRoomName logger manager config roomId = do
  result <- matrixGetState logger manager config roomId "m.room.name" :: IO (Either String MatrixRoomNameState)
  pure case result of
    Right value -> value.name
    Left _ -> Nothing

matrixRoomTopic :: Logger -> Manager -> MatrixConfig -> Text -> IO (Maybe Text)
matrixRoomTopic logger manager config roomId = do
  result <- matrixGetState logger manager config roomId "m.room.topic" :: IO (Either String MatrixRoomTopicState)
  pure case result of
    Right value -> value.topic
    Left _ -> Nothing

matrixGetState :: FromJSON a => Logger -> Manager -> MatrixConfig -> Text -> Text -> IO (Either String a)
matrixGetState logger manager config roomId eventType = do
  request <- authorizedRequest config (config.homeserver <> "/_matrix/client/v3/rooms/" <> roomId <> "/state/" <> eventType)
  response <- loggedHttp logger request manager
  pure (decodeMatrix response)

loggedHttp :: Logger -> Request -> Manager -> IO (Response LByteString.ByteString)
loggedHttp logger = HTTP.loggedHttp logger matrixHttpLogOptions

matrixHttpLogOptions :: HttpLogOptions
matrixHttpLogOptions =
  defaultHttpLogOptions {requestSuffix = " authorization=<redacted>"}

data MatrixRoomNameState = MatrixRoomNameState
  { name :: Maybe Text
  }

instance FromJSON MatrixRoomNameState where
  parseJSON = withObject "MatrixRoomNameState" \obj ->
    MatrixRoomNameState <$> obj .:? "name"

data MatrixRoomTopicState = MatrixRoomTopicState
  { topic :: Maybe Text
  }

instance FromJSON MatrixRoomTopicState where
  parseJSON = withObject "MatrixRoomTopicState" \obj ->
    MatrixRoomTopicState <$> obj .:? "topic"

data MatrixWhoami = MatrixWhoami
  { userId :: Text
  }

instance FromJSON MatrixWhoami where
  parseJSON = withObject "MatrixWhoami" \obj ->
    MatrixWhoami <$> obj .: "user_id"

data MatrixDisplayName = MatrixDisplayName
  { displayName :: Maybe Text
  }

instance FromJSON MatrixDisplayName where
  parseJSON = withObject "MatrixDisplayName" \obj ->
    MatrixDisplayName <$> obj .:? "displayname"

authorizedRequest :: MatrixConfig -> Text -> IO Request
authorizedRequest config url = do
  request <- parseRequest (Text.unpack url)
  pure
    request
      { requestHeaders = ("Authorization", Text.encodeUtf8 ("Bearer " <> config.accessToken)) : request.requestHeaders
      }

decodeMatrix :: FromJSON a => Response LByteString.ByteString -> Either String a
decodeMatrix response = eitherDecode response.responseBody

matrixUserKey :: Text -> PlatformKey
matrixUserKey userId = PlatformKey "matrix" "user" userId

matrixRoomKey :: Text -> PlatformKey
matrixRoomKey roomId = PlatformKey "matrix" "room" roomId

matrixEventKey :: Text -> PlatformKey
matrixEventKey matrixEventId = PlatformKey "matrix" "message" matrixEventId

transactionId :: EventId -> Text
transactionId (EventId eventId_) = eventId_

renderEventId :: EventId -> Text
renderEventId (EventId eventId_) = eventId_

escapePathSegment :: Text -> Text
escapePathSegment = Text.decodeUtf8 . urlEncode True . Text.encodeUtf8

plainMessage :: Text -> Message
plainMessage text = Text (Plain text :| []) :| []

readTimelineSeq :: Text -> Maybe TimelineSeq
readTimelineSeq = readMaybe . Text.unpack

data MatrixSync = MatrixSync
  { nextBatch :: Text
  , rooms :: [MatrixRoomEvents]
  }

instance FromJSON MatrixSync where
  parseJSON = withObject "MatrixSync" \obj -> do
    nextBatch <- obj .: "next_batch"
    roomsObject <- obj .:? "rooms" .!= mempty
    joinObject <- roomsObject .:? "join" .!= mempty
    rooms <- traverse parseRoom (Aeson.KeyMap.toList joinObject)
    pure MatrixSync {nextBatch, rooms}

parseRoom :: (Aeson.Key.Key, Value) -> Parser MatrixRoomEvents
parseRoom (roomKey, value) = withObject "JoinedRoom" parseJoinedRoom value
 where
  roomId = Aeson.Key.toText roomKey
  parseJoinedRoom obj = do
    timeline <- obj .:? "timeline" .!= mempty
    events <- timeline .:? "events" .!= []
    pure MatrixRoomEvents {roomId, events = mapMaybe usableEvent events}

data MatrixRoomEvents = MatrixRoomEvents
  { roomId :: Text
  , events :: [MatrixEvent]
  }

data MatrixEvent = MatrixEvent
  { eventId :: Text
  , sender :: Text
  , body :: Maybe Text
  }

usableEvent :: Value -> Maybe MatrixEvent
usableEvent value = do
  obj <- asObject value
  eventType <- Aeson.KeyMap.lookup "type" obj >>= asText
  guardMaybe (eventType == "m.room.message")
  matrixEventId <- Aeson.KeyMap.lookup "event_id" obj >>= asText
  sender <- Aeson.KeyMap.lookup "sender" obj >>= asText
  content <- Aeson.KeyMap.lookup "content" obj >>= asObject
  msgtype <- Aeson.KeyMap.lookup "msgtype" content >>= asText
  guardMaybe (msgtype == "m.text")
  body <- Aeson.KeyMap.lookup "body" content >>= asText
  pure MatrixEvent {eventId = matrixEventId, sender, body = Just body}

asObject :: Value -> Maybe Object
asObject = \case
  Object obj -> Just obj
  _ -> Nothing

asText :: Value -> Maybe Text
asText = \case
  String text -> Just text
  _ -> Nothing

guardMaybe :: Bool -> Maybe ()
guardMaybe True = Just ()
guardMaybe False = Nothing

data MatrixSendResponse = MatrixSendResponse
  { eventId :: Text
  }

instance FromJSON MatrixSendResponse where
  parseJSON = withObject "MatrixSendResponse" \obj ->
    MatrixSendResponse <$> obj .: "event_id"
