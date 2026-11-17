module Multiverse.QQ
  ( QQConfig (..)
  , qqBridge
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad (forever, unless)
import Data.Aeson
import Data.Aeson.Key qualified as Aeson.Key
import Data.Aeson.KeyMap qualified as Aeson.KeyMap
import Data.Aeson.Types (parseMaybe)
import Data.ByteString (ByteString)
import Data.ByteString.Base64 qualified as Base64
import Data.ByteString.Lazy qualified as LByteString
import Data.Foldable (toList)
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types.Status (statusCode)
import Network.WebSockets qualified as WebSockets
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Event hiding (eventId)
import Multiverse.HTTP (HttpLogOptions (..), defaultHttpLogOptions)
import Multiverse.HTTP qualified as HTTP
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data QQConfig = QQConfig
  { apiUrl :: Text
  , websocketUrl :: Text
  , accessToken :: Text
  , initialRooms :: [InitialRoomMapping]
  , noteInitialRoom :: InitialRoomMapping -> RoomId -> IO ()
  }

qqBridge :: Timeline timeline => QQConfig -> Bridge timeline
qqBridge config =
  Bridge
    { bridgeName = "qq"
    , observe = observeQQ config
    , reflect = reflectQQ config
    }

observeQQ :: Timeline timeline => QQConfig -> BridgeContext timeline -> IO ()
observeQQ config context = do
  manager <- newManager tlsManagerSettings
  ensureInitialRoomsWith
    config.initialRooms
    context
    (fetchInitialRoomInfo context.logger manager config)
    config.noteInitialRoom
  (host, port, path) <- parseWebSocketUrl config.websocketUrl
  let headers =
        if Text.null config.accessToken
          then []
          else [("Authorization", "Bearer " <> Text.Encoding.encodeUtf8 config.accessToken)]
  forever do
    result <- try (runQQWebSocket host port path headers manager config context) :: IO (Either SomeException ())
    case result of
      Left err -> logError context.logger ("qq observer disconnected: " <> Text.pack (show err))
      Right () -> logWarn context.logger "qq observer websocket closed"
    threadDelay 5000000

runQQWebSocket :: Timeline timeline => String -> Int -> String -> WebSockets.Headers -> Manager -> QQConfig -> BridgeContext timeline -> IO ()
runQQWebSocket host port path headers manager config context =
  WebSockets.runClientWith host port path WebSockets.defaultConnectionOptions headers \connection -> do
    logInfo context.logger "qq observer connected"
    forever do
      payload <- WebSockets.receiveData connection :: IO LByteString.ByteString
      case eitherDecode payload of
        Left err -> logWarn context.logger ("qq observer ignored invalid event: " <> Text.pack err)
        Right value ->
          case parseMaybe parseJSON value of
            Nothing -> logDebug context.logger "qq observer ignored unrecognized event"
            Just event -> observeQQEvent manager config context event

parseWebSocketUrl :: Text -> IO (String, Int, String)
parseWebSocketUrl url
  | not ("ws://" `Text.isPrefixOf` url) = fail "qq websocket_url must use ws://"
  | otherwise = do
      request <- parseRequest ("http://" <> Text.unpack (Text.drop 5 url))
      let host = Text.Encoding.decodeUtf8 request.host
          path = Text.Encoding.decodeUtf8 request.path <> Text.Encoding.decodeUtf8 request.queryString
      pure (Text.unpack host, request.port, Text.unpack path)

observeQQEvent :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> QQEvent -> IO ()
observeQQEvent manager config context event =
  case (event.qqPostType, event.qqNoticeType, event.qqMessageType) of
    (Just "message", _, Just "group") -> observeQQMessage manager config context event
    (Just "notice", Just "group_recall", _) -> observeQQRetraction config context event
    _ -> pure ()

observeQQMessage :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> QQEvent -> IO ()
observeQQMessage manager config context event =
  case (event.qqGroupId, event.qqUserId, event.qqMessageId) of
    (Just groupId, Just userId, Just messageId)
      | event.qqSelfId == Just userId ->
          logDebug context.logger ("qq ignoring own message " <> messageId)
      | otherwise -> do
          room <- configuredRoom config context groupId
          case room of
            Nothing -> logDebug context.logger ("qq ignoring unconfigured group " <> groupId)
            Just roomId -> do
              let messageKey = qqMessageKey groupId messageId
              existing <- context.mappingStore.lookupTimelineId messageKey
              case existing of
                Just _ -> logDebug context.logger ("qq ignoring duplicate message " <> messageId)
                Nothing -> do
                  let senderName = qqSenderName userId event.qqSender
                  sender <- ensureQQUser context userId senderName
                  segments <- pure (maybe [] messageSegments event.qqMessage)
                  decoded <- mapM (decodeQQSegment manager context roomId messageKey) (zip [0 ..] segments)
                  replyTo <- qqReplyTo context groupId (firstJust (map snd decoded))
                  let bodyParts = concatMap fst decoded
                      body = fromMaybe (plainMessage "") (nonEmpty bodyParts)
                      timelineEvent =
                        Event
                          { platformKey = messageKey
                          , content =
                              SendMessage
                                SendMessageInfo
                                  { sender
                                  , room = roomId
                                  , replyTo
                                  , forwardOf = Nothing
                                  , body
                                  }
                          }
                  submitted <- submitMessageMapped context timelineEvent
                  case submitted of
                    Right _ -> pure ()
                    Left err -> logError context.logger ("qq message submit error: " <> Text.pack (show err))
    _ -> logDebug context.logger "qq ignored group message with incomplete identifiers"

observeQQRetraction :: Timeline timeline => QQConfig -> BridgeContext timeline -> QQEvent -> IO ()
observeQQRetraction config context event =
  case (event.qqGroupId, event.qqMessageId) of
    (Just groupId, Just targetMessageId) -> do
      room <- configuredRoom config context groupId
      case room of
        Nothing -> pure ()
        Just _ -> do
          target <- context.mappingStore.lookupTimelineId (qqMessageKey groupId targetMessageId)
          case target of
            Nothing -> logDebug context.logger ("qq ignoring recall for unknown message " <> targetMessageId)
            Just targetId -> do
              let key = PlatformKey "qq" "retraction" (groupId <> "/" <> targetMessageId)
                  eventToSubmit = Event {platformKey = key, content = RetractMessage (MessageId targetId)}
              result <- submitMapped context eventToSubmit
              case result of
                Left err -> logError context.logger ("qq retraction submit error: " <> Text.pack (show err))
                Right _ -> pure ()
    _ -> logDebug context.logger "qq ignored recall event with incomplete identifiers"

configuredRoom :: QQConfig -> BridgeContext timeline -> Text -> IO (Maybe RoomId)
configuredRoom config context groupId = do
  let key = qqRoomKey groupId
  if key `elem` map (.platformKey) config.initialRooms
    then fmap RoomId <$> context.mappingStore.lookupTimelineId key
    else pure Nothing

qqReplyTo :: BridgeContext timeline -> Text -> Maybe Text -> IO (Maybe MessageId)
qqReplyTo _ _ Nothing = pure Nothing
qqReplyTo context groupId (Just messageId) = do
  mapped <- context.mappingStore.lookupTimelineId (qqMessageKey groupId messageId)
  pure (MessageId <$> mapped)

ensureQQUser :: Timeline timeline => BridgeContext timeline -> Text -> Text -> IO UserId
ensureQQUser context qqUserId displayName = do
  let key = qqUserKey qqUserId
  existing <- context.mappingStore.lookupTimelineId key
  case existing of
    Just eventId -> pure (UserId eventId)
    Nothing -> do
      let userName = if Text.null displayName then qqUserId else displayName
          event = Event {platformKey = key, content = CreateUser UserInfo {name = userName, avatar = Nothing}}
      mapped <- submitMapped context event
      case mapped of
        Right eventId -> pure (UserId eventId)
        Left err -> fail ("could not create qq user: " <> show err)

decodeQQSegment :: Timeline timeline => Manager -> BridgeContext timeline -> RoomId -> PlatformKey -> (Int, Value) -> IO ([MessagePart], Maybe Text)
decodeQQSegment manager context room messageKey (index, segment) = do
  let segmentType = valueTextField "type" segment
      segmentData = fromMaybe Null (valueField "data" segment)
  case segmentType of
    Just "text" -> pure (oneTextPart (fromMaybe "" (valueTextField "text" segmentData)), Nothing)
    Just "at" -> do
      let targetId = valueTextField "qq" segmentData
      case targetId of
        Just "all" -> pure ([Text (Mention (Plain "@all" :| []) (Right room) :| [])], Nothing)
        Just userId -> do
          user <- ensureQQUser context userId userId
          let label = Plain ("@" <> userId) :| []
          pure ([Text (Mention label (Left user) :| [])], Nothing)
        Nothing -> pure (oneTextPart "[mention]", Nothing)
    Just "reply" -> pure ([], valueTextField "id" segmentData)
    Just kind | kind `elem` ["image", "mface", "record", "video"] -> do
      let blobType = case kind of
            "record" -> AudioBlob
            "video" -> VideoBlob
            _ -> ImageBlob
          url = valueTextField "url" segmentData <|> valueTextField "file" segmentData
      blob <- traverse (importQQBlob manager context messageKey index) url
      case blob of
        Just (Just blobId) -> pure ([Blob blobType blobId], Nothing)
        _ -> pure (oneTextPart ("[" <> kind <> "]"), Nothing)
    Just "face" ->
      let faceId = fromMaybe "?" (valueTextField "id" segmentData)
       in pure (oneTextPart ("[face:" <> faceId <> "]"), Nothing)
    Just "share" ->
      let title = fromMaybe "link" (valueTextField "title" segmentData)
          url = fromMaybe "" (valueTextField "url" segmentData)
       in pure (oneTextPart (title <> if Text.null url then "" else " (" <> url <> ")"), Nothing)
    Just kind -> pure (oneTextPart ("[" <> kind <> "]"), Nothing)
    Nothing -> pure (oneTextPart "[segment]", Nothing)

importQQBlob :: Timeline timeline => Manager -> BridgeContext timeline -> PlatformKey -> Int -> Text -> IO (Maybe BlobId)
importQQBlob manager context messageKey index mediaUrl = do
  existing <- context.mappingStore.lookupTimelineId blobKey
  case existing of
    Just eventId -> pure (Just (BlobId eventId))
    Nothing -> do
      bytes <- downloadQQMedia manager context.logger mediaUrl
      case bytes of
        Nothing -> pure Nothing
        Just content -> do
          let event = Event {platformKey = blobKey, content = CreateBlob content}
          result <- submitMapped context event
          case result of
            Right eventId -> pure (Just (BlobId eventId))
            Left err -> do
              logError context.logger ("qq media submit error: " <> Text.pack (show err))
              pure Nothing
 where
  blobKey = qqBlobKey messageKey index

downloadQQMedia :: Manager -> Logger -> Text -> IO (Maybe ByteString)
downloadQQMedia manager logger mediaUrl = do
  parsed <- try (parseRequest (Text.unpack mediaUrl)) :: IO (Either HttpException Request)
  case parsed of
    Left err -> logWarn logger ("qq media URL rejected: " <> Text.pack (show err)) >> pure Nothing
    Right request0 -> do
      let host = Text.toLower (Text.Encoding.decodeUtf8 request0.host)
      if not request0.secure || not (isQQMediaHost host)
        then do
          logWarn logger "qq media URL rejected by HTTPS host allowlist"
          pure Nothing
        else download 1 host request0 {redirectCount = 0}
 where
  download attempt host request = do
    logDebug logger ("qq media GET host=" <> host)
    result <- try (httpLbs request manager) :: IO (Either HttpException (Response LByteString.ByteString))
    case result of
      Left err
        | attempt < 3 -> do
            logWarn logger ("qq media request failed; retrying: " <> Text.pack (show err))
            threadDelay (attempt * 1000000)
            download (attempt + 1) host request
        | otherwise -> logError logger ("qq media request failed: " <> Text.pack (show err)) >> pure Nothing
      Right response -> do
        let bytes = response.responseBody
            size = LByteString.length bytes
            status = statusCode response.responseStatus
        logDebug logger ("qq media response status=" <> Text.pack (show status) <> " bytes=" <> Text.pack (show size))
        if status >= 200 && status < 300 && size <= 8000000
          then pure (Just (LByteString.toStrict bytes))
          else do
            logWarn logger "qq media response rejected (status or size limit)"
            pure Nothing

isQQMediaHost :: Text -> Bool
isQQMediaHost host = any matches ["qq.com", "qpic.cn", "gtimg.cn"]
 where
  matches suffix = host == suffix || ("." <> suffix) `Text.isSuffixOf` host

reflectQQ :: Timeline timeline => QQConfig -> BridgeContext timeline -> IO ()
reflectQQ config context = do
  manager <- newManager tlsManagerSettings
  forever do
    afterEventId <- fmap (fmap EventId) (context.mappingStore.lookupState "qq.reflect_event")
    storedEvents <- getEventsAfter context.timeline afterEventId
    mapM_ (reflectQQEvent manager config context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "qq.reflect_event" (renderEventId ((last storedEvents).storedId))

reflectQQEvent :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> StoredEvent -> IO ()
reflectQQEvent manager config context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
          mapM_ (sendToQQRoom manager config context stored info) (filter isQQRoom roomKeys)
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin retraction " <> renderEventId stored.storedId)
      | otherwise -> retractQQMessages manager config context stored message
    _ -> pure ()
 where
  isQQRoom key = key.platform == "qq" && key.entityType == "room"

sendToQQRoom :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> StoredEvent -> SendMessageInfo -> PlatformKey -> IO ()
sendToQQRoom manager config context stored info roomKey =
  case readMaybe (Text.unpack roomKey.key) :: Maybe Integer of
    Nothing -> logWarn context.logger ("qq ignoring invalid group id " <> roomKey.key)
    Just groupId -> do
      alreadySent <- qqMessageMappedToGroup context stored.storedId roomKey.key
      unless alreadySent do
        body <- renderQQMessage context roomKey.key info
        replyTo <- qqReplyMessageId context roomKey.key info.replyTo
        let segments = maybe body (\messageId -> qqReplySegment messageId : body) replyTo
            params = object ["group_id" .= groupId, "message" .= segments]
        response <- qqApiCall context.logger manager config "send_group_msg" params
        case response of
          Left err -> fail ("qq send_group_msg failed: " <> err)
          Right result ->
            case valueTextField "message_id" result of
              Nothing -> fail "qq send_group_msg response lacked message_id"
              Just messageId ->
                context.mappingStore.insertMapping (qqMessageKey roomKey.key messageId) stored.storedId

qqMessageMappedToGroup :: BridgeContext timeline -> EventId -> Text -> IO Bool
qqMessageMappedToGroup context eventId groupId = do
  keys <- context.mappingStore.lookupPlatformKeys eventId
  pure (any (\key -> key.platform == "qq" && key.entityType == "message" && messageGroupId key == Just groupId) keys)

qqReplyMessageId :: BridgeContext timeline -> Text -> Maybe MessageId -> IO (Maybe Text)
qqReplyMessageId _ _ Nothing = pure Nothing
qqReplyMessageId context groupId (Just message) = do
  keys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
  pure (firstJust (map (qqMessageIdForGroup groupId) keys))

qqMessageIdForGroup :: Text -> PlatformKey -> Maybe Text
qqMessageIdForGroup groupId key =
  case Text.splitOn "/" key.key of
    [mappedGroupId, messageId]
      | key.platform == "qq" && key.entityType == "message" && mappedGroupId == groupId -> Just messageId
    _ -> Nothing

retractQQMessages :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> StoredEvent -> MessageId -> IO ()
retractQQMessages manager config context stored message = do
  original <- getEvent context.timeline (messageEventId message)
  case fmap (.storedEvent.content) original of
    Just (SendMessage _) -> do
      keys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
      mapM_ (deleteQQMessage manager config context stored) (mapMaybe qqMessageTarget keys)
    _ -> pure ()

qqMessageTarget :: PlatformKey -> Maybe (Text, Text)
qqMessageTarget key
  | key.platform /= "qq" || key.entityType /= "message" = Nothing
  | otherwise =
      case Text.splitOn "/" key.key of
        [groupId, messageId] -> Just (groupId, messageId)
        _ -> Nothing

deleteQQMessage :: Manager -> QQConfig -> BridgeContext timeline -> StoredEvent -> (Text, Text) -> IO ()
deleteQQMessage manager config context stored (groupId, messageId) = do
  let key = qqRetractionKey groupId stored.storedId
  alreadyDeleted <- context.mappingStore.lookupTimelineId key
  case alreadyDeleted of
    Just _ -> pure ()
    Nothing -> do
      response <- qqApiCall context.logger manager config "delete_msg" (object ["message_id" .= qqIdJson messageId])
      case response of
        Left err -> logError context.logger ("qq delete_msg failed: " <> Text.pack err)
        Right _ -> context.mappingStore.insertMapping key stored.storedId

renderQQMessage :: Timeline timeline => BridgeContext timeline -> Text -> SendMessageInfo -> IO [Value]
renderQQMessage context groupId info = do
  senderInfo <- getUserInfo context.timeline info.sender (Just info.room)
  let senderName = maybe "unknown" (.name) senderInfo
  body <- renderQQMessageBody context groupId info.room info.body
  pure (qqTextSegment (senderName <> ": ") : body)

renderQQMessageBody :: Timeline timeline => BridgeContext timeline -> Text -> RoomId -> Message -> IO [Value]
renderQQMessageBody context groupId room = fmap concat . mapM (renderQQMessagePart context groupId room) . toList

renderQQMessagePart :: Timeline timeline => BridgeContext timeline -> Text -> RoomId -> MessagePart -> IO [Value]
renderQQMessagePart context groupId room = \case
  Text inline -> renderQQInline context groupId room inline
  Emote inline -> (qqTextSegment "* " :) <$> renderQQInline context groupId room inline
  Blob blobType blobId -> renderQQBlob context blobType blobId
  List messages -> concat <$> mapM (\message -> (qqTextSegment "- " :) <$> renderQQMessageBody context groupId room message) (toList messages)
  BlockQuote message -> (qqTextSegment "> " :) <$> renderQQMessageBody context groupId room message
  Heading level inline ->
    let safeLevel = max 1 (min 6 level)
     in (qqTextSegment (Text.replicate safeLevel "#" <> " ") :) <$> renderQQInline context groupId room inline

renderQQInline :: Timeline timeline => BridgeContext timeline -> Text -> RoomId -> InlineText -> IO [Value]
renderQQInline context groupId room = fmap concat . mapM (renderQQInlinePart context groupId room) . toList

renderQQInlinePart :: Timeline timeline => BridgeContext timeline -> Text -> RoomId -> InlineTextPart -> IO [Value]
renderQQInlinePart context groupId room = \case
  Bold inline -> wrapInline "*" inline
  Italic inline -> wrapInline "_" inline
  Link inline url -> do
    rendered <- renderQQInline context groupId room inline
    pure (rendered <> [qqTextSegment " (" , qqTextSegment url, qqTextSegment ")"])
  Mention inline (Left user) -> do
    keys <- context.mappingStore.lookupPlatformKeys (userEventId user)
    case [key.key | key <- keys, key.platform == "qq", key.entityType == "user"] of
      qqUserId : _ -> pure [object ["type" .= ("at" :: Text), "data" .= object ["qq" .= qqUserId]]]
      [] -> renderQQInline context groupId room inline
  Mention inline (Right mentionedRoom)
    | roomEventId mentionedRoom == roomEventId room ->
        pure [object ["type" .= ("at" :: Text), "data" .= object ["qq" .= ("all" :: Text)]]]
    | otherwise -> renderQQInline context groupId room inline
  InlineQuote inline -> do
    rendered <- renderQQInline context groupId room inline
    pure ([qqTextSegment "\"" ] <> rendered <> [qqTextSegment "\""])
  Plain text -> pure [qqTextSegment text]
 where
  wrapInline marker inline = do
    rendered <- renderQQInline context groupId room inline
    pure ([qqTextSegment marker] <> rendered <> [qqTextSegment marker])

renderQQBlob :: Timeline timeline => BridgeContext timeline -> BlobType -> BlobId -> IO [Value]
renderQQBlob context blobType blobId = do
  stored <- getEvent context.timeline (blobEventId blobId)
  case fmap (.storedEvent.content) stored of
    Just (CreateBlob bytes) ->
      let kind = case blobType of
            ImageBlob -> "image"
            VideoBlob -> "video"
            AudioBlob -> "record"
          file = "base64://" <> Text.Encoding.decodeUtf8 (Base64.encode bytes)
       in pure [object ["type" .= (kind :: Text), "data" .= object ["file" .= file]]]
    _ -> pure [qqTextSegment "[blob]"]

fetchInitialRoomInfo :: Logger -> Manager -> QQConfig -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo logger manager config mapping = do
  response <- qqApiCall logger manager config "get_group_info" (object ["group_id" .= qqIdJson mapping.platformKey.key])
  case response of
    Left err -> fail ("qq get_group_info failed: " <> err)
    Right info ->
      pure
        RoomInfo
          { name = fromMaybe mapping.platformKey.key (valueTextField "group_name" info)
          , description = ""
          , avatar = Nothing
          }

qqApiCall :: Logger -> Manager -> QQConfig -> Text -> Value -> IO (Either String Value)
qqApiCall logger manager config action parameters = do
  request0 <- parseRequest (Text.unpack (Text.dropWhileEnd (== '/') config.apiUrl <> "/" <> action))
  let authHeader =
        if Text.null config.accessToken
          then []
          else [("Authorization", Text.Encoding.encodeUtf8 ("Bearer " <> config.accessToken))]
      request =
        request0
          { method = "POST"
          , requestBody = RequestBodyLBS (encode parameters)
          , requestHeaders = ("Content-Type", "application/json") : authHeader <> request0.requestHeaders
          }
  response <- HTTP.loggedHttp logger qqHttpLogOptions request manager
  pure (decodeQQApiResponse response)

qqHttpLogOptions :: HttpLogOptions
qqHttpLogOptions = defaultHttpLogOptions {requestSuffix = " authorization=<redacted>"}

decodeQQApiResponse :: Response LByteString.ByteString -> Either String Value
decodeQQApiResponse response = do
  envelope <- eitherDecode response.responseBody :: Either String QQApiResponse
  case envelope.qqRetcode of
    Just 0 -> Right (fromMaybe (object []) envelope.qqData)
    Just code -> Left ("retcode " <> show code <> ": " <> Text.unpack (fromMaybe "OneBot action failed" envelope.qqMessage))
    Nothing -> Left "OneBot response lacked retcode"

data QQApiResponse = QQApiResponse
  { qqRetcode :: Maybe Int
  , qqData :: Maybe Value
  , qqMessage :: Maybe Text
  }

instance FromJSON QQApiResponse where
  parseJSON = withObject "OneBot response" \obj ->
    QQApiResponse <$> obj .:? "retcode" <*> obj .:? "data" <*> (obj .:? "message" <|> obj .:? "wording")

data QQEvent = QQEvent
  { qqPostType :: Maybe Text
  , qqNoticeType :: Maybe Text
  , qqMessageType :: Maybe Text
  , qqGroupId :: Maybe Text
  , qqUserId :: Maybe Text
  , qqSelfId :: Maybe Text
  , qqMessageId :: Maybe Text
  , qqMessage :: Maybe Value
  , qqSender :: Maybe QQSender
  }

instance FromJSON QQEvent where
  parseJSON = withObject "OneBot event" \obj -> do
    sender <- obj .:? "sender"
    pure
      QQEvent
        { qqPostType = valueTextField "post_type" (Object obj)
        , qqNoticeType = valueTextField "notice_type" (Object obj)
        , qqMessageType = valueTextField "message_type" (Object obj)
        , qqGroupId = valueTextField "group_id" (Object obj)
        , qqUserId = valueTextField "user_id" (Object obj)
        , qqSelfId = valueTextField "self_id" (Object obj)
        , qqMessageId = valueTextField "message_id" (Object obj)
        , qqMessage = Aeson.KeyMap.lookup "message" obj
        , qqSender = sender
        }

data QQSender = QQSender
  { qqNickname :: Maybe Text
  , qqCard :: Maybe Text
  }

instance FromJSON QQSender where
  parseJSON = withObject "OneBot sender" \obj ->
    QQSender <$> obj .:? "nickname" <*> obj .:? "card"

messageSegments :: Value -> [Value]
messageSegments = \case
  Array values -> toList values
  String text -> [object ["type" .= ("text" :: Text), "data" .= object ["text" .= text]]]
  _ -> []

valueField :: Text -> Value -> Maybe Value
valueField key (Object obj) = Aeson.KeyMap.lookup (Aeson.Key.fromText key) obj
valueField _ _ = Nothing

valueTextField :: Text -> Value -> Maybe Text
valueTextField key value = valueField key value >>= scalarText

scalarText :: Value -> Maybe Text
scalarText = \case
  String text -> Just text
  Number number ->
    let integer = truncate number :: Integer
     in if fromInteger integer == number
          then Just (Text.pack (show integer))
          else Nothing
  _ -> Nothing

qqSenderName :: Text -> Maybe QQSender -> Text
qqSenderName userId sender =
  fromMaybe userId
    ( firstNonempty
        [ sender >>= (.qqCard)
        , sender >>= (.qqNickname)
        ]
    )

firstNonempty :: [Maybe Text] -> Maybe Text
firstNonempty = \case
  [] -> Nothing
  Just text : _ | not (Text.null text) -> Just text
  _ : rest -> firstNonempty rest

firstJust :: [Maybe a] -> Maybe a
firstJust = \case
  [] -> Nothing
  Just value : _ -> Just value
  _ : rest -> firstJust rest

oneTextPart :: Text -> [MessagePart]
oneTextPart text = [Text (Plain text :| [])]

plainMessage :: Text -> Message
plainMessage text = Text (Plain text :| []) :| []

qqTextSegment :: Text -> Value
qqTextSegment text = object ["type" .= ("text" :: Text), "data" .= object ["text" .= text]]

qqReplySegment :: Text -> Value
qqReplySegment messageId = object ["type" .= ("reply" :: Text), "data" .= object ["id" .= messageId]]

qqIdJson :: Text -> Value
qqIdJson text = maybe (String text) toJSON (readMaybe (Text.unpack text) :: Maybe Integer)

qqRoomKey :: Text -> PlatformKey
qqRoomKey groupId = PlatformKey "qq" "room" groupId

qqUserKey :: Text -> PlatformKey
qqUserKey userId = PlatformKey "qq" "user" userId

qqMessageKey :: Text -> Text -> PlatformKey
qqMessageKey groupId messageId = PlatformKey "qq" "message" (groupId <> "/" <> messageId)

qqBlobKey :: PlatformKey -> Int -> PlatformKey
qqBlobKey messageKey index = PlatformKey "qq" "blob" (messageKey.key <> "/" <> Text.pack (show index))

qqRetractionKey :: Text -> EventId -> PlatformKey
qqRetractionKey groupId eventId_ = PlatformKey "qq" "retraction" (groupId <> "/" <> renderEventId eventId_)

messageGroupId :: PlatformKey -> Maybe Text
messageGroupId key =
  case Text.splitOn "/" key.key of
    groupId : _ -> Just groupId
    [] -> Nothing

renderEventId :: EventId -> Text
renderEventId (EventId eventId_) = eventId_
