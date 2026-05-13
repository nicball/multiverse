module Multiverse.Matrix
  ( MatrixConfig (..)
  , matrixBridge
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (try)
import Control.Monad (forever, unless)
import Data.Aeson
import Data.Aeson.Types (Parser)
import Data.Aeson.Key qualified as Aeson.Key
import Data.Aeson.KeyMap qualified as Aeson.KeyMap
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.Text.Encoding.Error (lenientDecode)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.List.NonEmpty (toList)
import Data.Maybe (mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types (statusCode)
import Network.HTTP.Types.URI (urlEncode)
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Event hiding (eventId)
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
          submitted <- submit context.timeline timelineEvent
          case submitted of
            Right timelineEventId -> context.mappingStore.insertMapping platformKey timelineEventId
            Left (ConflictingPlatformKey _ timelineEventId) -> context.mappingStore.insertMapping platformKey timelineEventId
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
      submitted <- submit context.timeline event
      case submitted of
        Right eventId -> context.mappingStore.insertMapping platformKey eventId >> pure (UserId eventId)
        Left (ConflictingPlatformKey _ eventId) -> context.mappingStore.insertMapping platformKey eventId >> pure (UserId eventId)
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

hasPlatformMapping :: MappingStore -> Text -> EventId -> IO Bool
hasPlatformMapping store platform mappedEventId = any ((== platform) . (.platform)) <$> store.lookupPlatformKeys mappedEventId

renderRelayedMessage :: Timeline timeline => BridgeContext timeline -> SendMessageInfo -> IO Text
renderRelayedMessage context info = do
  user <- getUserInfo context.timeline info.sender (Just info.room)
  let sender = maybe "unknown" (.name) user
  pure (sender <> ": " <> renderMessageText info.body)

renderMessageText :: Message -> Text
renderMessageText = Text.intercalate "\n" . map renderMessagePart . toList

renderMessagePart :: MessagePart -> Text
renderMessagePart = \case
  Text inline -> renderInline inline
  Emote inline -> "_ " <> renderInline inline
  Blob _ _ -> "[blob]"
  List items -> Text.intercalate "\n" (map renderMessageText (toList items))
  BlockQuote message -> "> " <> Text.replace "\n" "\n> " (renderMessageText message)

renderInline :: InlineText -> Text
renderInline = Text.concat . map renderInlinePart . toList

renderInlinePart :: InlineTextPart -> Text
renderInlinePart = \case
  Bold inline -> renderInline inline
  Italic inline -> renderInline inline
  Link inline url -> renderInline inline <> " (" <> url <> ")"
  Mention inline _ -> renderInline inline
  InlineQuote inline -> "\"" <> renderInline inline <> "\""
  Plain text -> text

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
loggedHttp logger request manager = do
  logDebug logger ("http request " <> requestSummary request)
  response <- httpLbsWithBackoff logger request manager
  logDebug logger ("http response " <> responseSummary request response)
  pure response

httpLbsWithBackoff :: Logger -> Request -> Manager -> IO (Response LByteString.ByteString)
httpLbsWithBackoff logger request manager =
  go initialHttpBackoffMicros
 where
  go delayMicros = do
    result <- try (httpLbs request manager)
    case result of
      Right response -> pure response
      Left (err :: HttpException) -> do
        logWarn logger ("http request failed, retrying in " <> delayText delayMicros <> ": " <> Text.pack (show err))
        threadDelay delayMicros
        go (min maxHttpBackoffMicros (delayMicros * 2))

initialHttpBackoffMicros :: Int
initialHttpBackoffMicros = 1000000

maxHttpBackoffMicros :: Int
maxHttpBackoffMicros = 60000000

delayText :: Int -> Text
delayText micros = Text.pack (show (micros `div` 1000000)) <> "s"

requestSummary :: Request -> Text
requestSummary request =
  Text.pack (ByteString.Char8.unpack request.method)
    <> " "
    <> requestUrl request
    <> " request_body="
    <> requestBodyPreview request.requestBody
    <> " authorization=<redacted>"

responseSummary :: Request -> Response LByteString.ByteString -> Text
responseSummary request response =
  Text.pack (ByteString.Char8.unpack request.method)
    <> " "
    <> requestUrl request
    <> " status="
    <> Text.pack (show (statusCode response.responseStatus))
    <> " response_body="
    <> truncateBody response.responseBody

requestBodyPreview :: RequestBody -> Text
requestBodyPreview = \case
  RequestBodyLBS body -> truncateBody body
  RequestBodyBS body -> truncateBody (LByteString.fromStrict body)
  RequestBodyBuilder size _ -> "builder:" <> Text.pack (show size) <> " bytes"
  RequestBodyStream size _ -> "stream:" <> Text.pack (show size) <> " bytes"
  RequestBodyStreamChunked _ -> "chunked"
  RequestBodyIO _ -> "io"

truncateBody :: LByteString.ByteString -> Text
truncateBody body =
  let text = Text.decodeUtf8With lenientDecode (LByteString.toStrict (LByteString.take 512 body))
   in if LByteString.length body > 512 then text <> "...<truncated>" else text

requestUrl :: Request -> Text
requestUrl request =
  Text.decodeUtf8 (securePrefix request)
    <> Text.decodeUtf8 request.host
    <> portText request
    <> Text.decodeUtf8 request.path
    <> Text.decodeUtf8 request.queryString

securePrefix :: Request -> ByteString.Char8.ByteString
securePrefix request = if request.secure then "https://" else "http://"

portText :: Request -> Text
portText request =
  let defaultPort = if request.secure then 443 else 80
   in if request.port == defaultPort then "" else ":" <> Text.pack (show request.port)

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
