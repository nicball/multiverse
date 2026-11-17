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
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
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
    (Just "message", _, Just "private") -> logDebug context.logger "qq ignoring private message"
    (Just "message_sent", _, _) -> logDebug context.logger "qq ignoring echoed message"
    (Just "notice", Just "group_recall", _) -> observeQQRetraction config context event
    (Just "notice", Just "group_increase", _) -> observeQQMembership manager config context event True
    (Just "notice", Just "group_decrease", _) -> observeQQMembership manager config context event False
    (Just "notice", Just "group_kick", _) -> observeQQMembership manager config context event False
    (Just "notice", Just "group_card", _) -> observeQQCard config context event
    (Just postType, Just noticeType, _) ->
      logDebug context.logger ("qq ignoring notice " <> postType <> "/" <> noticeType)
    _ -> logDebug context.logger "qq ignoring unrecognized event"

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
                  ensureRoomMembership context "qq" roomId sender
                  let messageContext =
                        QQContext
                          { manager
                          , config
                          , bridge = context
                          , room = roomId
                          , groupId
                          , blobPrefix = messageKey.key
                          }
                  (bodyParts, replyId) <- decodeQQSegments messageContext (fromMaybe Null event.qqMessage)
                  replyTo <- qqReplyTo context groupId replyId
                  let body = fromMaybe (plainMessage "") (nonEmpty bodyParts)
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

observeQQMembership :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> QQEvent -> Bool -> IO ()
observeQQMembership manager config context event joined =
  case (event.qqGroupId, event.qqUserId) of
    (Just groupId, Just userId)
      | event.qqSelfId == Just userId ->
          logDebug context.logger ("qq ignoring own membership " <> userId)
      | otherwise -> do
          room <- configuredRoom config context groupId
          case room of
            Nothing -> pure ()
            Just roomId -> do
              name <- qqGroupMemberName manager config context.logger groupId userId (qqSenderName userId event.qqSender)
              user <- ensureQQUser context userId name
              let key = PlatformKey "qq" (if joined then "join" else "leave") (noticeKey groupId userId event.qqTime)
                  content = if joined then JoinRoom user roomId else LeaveRoom user roomId
              result <- submitMapped context Event {platformKey = key, content}
              case result of
                Left err -> logError context.logger ("qq membership submit error: " <> Text.pack (show err))
                Right _ -> pure ()
    _ -> logDebug context.logger "qq ignored membership notice with incomplete identifiers"

observeQQCard :: Timeline timeline => QQConfig -> BridgeContext timeline -> QQEvent -> IO ()
observeQQCard config context event =
  case (event.qqGroupId, event.qqUserId, event.qqCardNew) of
    (Just groupId, Just userId, Just card)
      | event.qqSelfId == Just userId ->
          logDebug context.logger ("qq ignoring own nickname change " <> userId)
      | not (Text.null card) -> do
          room <- configuredRoom config context groupId
          case room of
            Nothing -> pure ()
            Just roomId -> do
              let senderName = qqSenderName userId event.qqSender
              user <- ensureQQUser context userId (if senderName == userId then card else senderName)
              let key = PlatformKey "qq" "nick" (noticeKey groupId userId event.qqTime)
              result <- submitMapped context Event {platformKey = key, content = ChangeRoomNick user roomId card}
              case result of
                Left err -> logError context.logger ("qq nickname submit error: " <> Text.pack (show err))
                Right _ -> pure ()
    _ -> logDebug context.logger "qq ignored card notice with incomplete identifiers"

noticeKey :: Text -> Text -> Maybe Text -> Text
noticeKey groupId userId time =
  case time of
    Just stamp -> groupId <> "/" <> userId <> "/" <> stamp
    Nothing -> groupId <> "/" <> userId

-- | Notices often carry no sender, so ask the platform for a display name.
qqGroupMemberName :: Manager -> QQConfig -> Logger -> Text -> Text -> Text -> IO Text
qqGroupMemberName manager config logger groupId userId fallback
  | fallback /= userId = pure fallback
  | otherwise = do
      response <-
        qqApiCall
          logger
          manager
          config
          "get_group_member_info"
          (object ["group_id" .= qqIdJson groupId, "user_id" .= qqIdJson userId])
      case response of
        Left err -> do
          logDebug logger ("qq get_group_member_info failed: " <> Text.pack err)
          pure fallback
        Right info ->
          pure (fromMaybe fallback (firstNonempty [valueTextField "card" info, valueTextField "nickname" info]))

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
  if enabledRoom config.initialRooms key
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

data QQContext timeline = QQContext
  { manager :: Manager
  , config :: QQConfig
  , bridge :: BridgeContext timeline
  , room :: RoomId
  , groupId :: Text
  , blobPrefix :: Text
  }

decodeQQSegments :: Timeline timeline => QQContext timeline -> Value -> IO ([MessagePart], Maybe Text)
decodeQQSegments context value = do
  decoded <- mapM (uncurry (decodeQQSegment context)) (zip [0 ..] (messageSegments value))
  pure (concatMap fst decoded, firstJust (map snd decoded))

decodeQQSegment :: Timeline timeline => QQContext timeline -> Int -> Value -> IO ([MessagePart], Maybe Text)
decodeQQSegment context index segment = do
  let segmentType = valueTextField "type" segment
      segmentData = fromMaybe Null (valueField "data" segment)
  case segmentType of
    Nothing -> pure (oneTextPart "[segment]", Nothing)
    Just "text" ->
      pure (oneTextPart (fromMaybe "" (valueTextField "text" segmentData)), Nothing)
    Just "at" -> (, Nothing) <$> decodeQQMention context segmentData
    Just "reply" -> pure ([], valueTextField "id" segmentData)
    Just "image" -> do
      blob <- importQQSegmentBlob context index segmentData
      let flash = valueTextField "type" segmentData == Just "flash"
          parts = qqBlobParts ImageBlob blob "image"
      pure ((if flash then oneTextPart "[flash]" else []) <> parts, Nothing)
    Just "mface" -> do
      blob <- importQQSegmentBlob context index segmentData
      let label = fromMaybe "mface" (valueTextField "summary" segmentData)
      pure (qqBlobParts ImageBlob blob label, Nothing)
    Just "record" -> decodeQQTypedBlob context index AudioBlob "record" segmentData
    Just "video" -> decodeQQTypedBlob context index VideoBlob "video" segmentData
    Just "cardimage" -> decodeQQTypedBlob context index ImageBlob "cardimage" segmentData
    Just "file" -> do
      blob <- importQQSegmentBlob context index segmentData
      let label =
            fromMaybe
              "file"
              (firstNonempty [valueTextField "name" segmentData, valueTextField "file_name" segmentData])
      pure (qqBlobParts FileBlob blob label, Nothing)
    Just "face" ->
      pure (oneTextPart ("[face:" <> fromMaybe "?" (valueTextField "id" segmentData) <> "]"), Nothing)
    Just "rps" -> pure (oneTextPart "[rps]", Nothing)
    Just "dice" -> pure (oneTextPart "[dice]", Nothing)
    Just "shake" -> pure (oneTextPart "[shake]", Nothing)
    Just "poke" -> pure (oneTextPart "[poke]", Nothing)
    Just "anonymous" -> pure ([], Nothing)
    Just "keyboard" -> pure ([], Nothing)
    Just "tts" -> pure (oneTextPart (fromMaybe "[tts]" (valueTextField "text" segmentData)), Nothing)
    Just "share" -> pure (oneInlinePart (qqShareInline segmentData), Nothing)
    Just "contact" -> pure (oneTextPart (qqContactText segmentData), Nothing)
    Just "location" -> pure (oneInlinePart (qqLocationInline segmentData), Nothing)
    Just "music" -> pure (oneInlinePart (qqMusicInline segmentData), Nothing)
    Just "json" -> pure (oneTextPart (qqJsonCardText segmentData), Nothing)
    Just "xml" -> pure (oneTextPart "[card]", Nothing)
    Just "markdown" -> pure (oneTextPart (qqMarkdownText segmentData), Nothing)
    Just "forward" -> (, Nothing) <$> decodeQQForward context index segmentData
    Just "node" -> (, Nothing) <$> decodeQQNodes context (forwardPrefix context index) segmentData
    Just other -> do
      logDebug context.bridge.logger ("qq ignoring unsupported segment type " <> other)
      pure (oneTextPart ("[" <> other <> "]"), Nothing)

decodeQQTypedBlob :: Timeline timeline => QQContext timeline -> Int -> BlobType -> Text -> Value -> IO ([MessagePart], Maybe Text)
decodeQQTypedBlob context index blobType label segmentData = do
  blob <- importQQSegmentBlob context index segmentData
  pure (qqBlobParts blobType blob label, Nothing)

qqBlobParts :: BlobType -> Maybe BlobId -> Text -> [MessagePart]
qqBlobParts blobType (Just blobId) _ = [Blob blobType blobId]
qqBlobParts _ Nothing label = oneTextPart ("[" <> label <> "]")

decodeQQMention :: Timeline timeline => QQContext timeline -> Value -> IO [MessagePart]
decodeQQMention context segmentData =
  case valueTextField "qq" segmentData of
    Just "all" -> pure (oneInlinePart (Mention (Plain "@all" :| []) (Right context.room)))
    Just userId -> do
      let name = fromMaybe userId (firstNonempty [valueTextField "name" segmentData])
      user <- ensureQQUser context.bridge userId name
      pure (oneInlinePart (Mention (Plain ("@" <> name) :| []) (Left user)))
    Nothing -> pure (oneTextPart "[mention]")

importQQSegmentBlob :: Timeline timeline => QQContext timeline -> Int -> Value -> IO (Maybe BlobId)
importQQSegmentBlob context index segmentData =
  case firstHttpUrl [valueTextField "url" segmentData, valueTextField "file" segmentData, valueTextField "path" segmentData] of
    Nothing -> pure Nothing
    Just mediaUrl -> do
      let blobKey = qqBlobKey context.blobPrefix index
      existing <- context.bridge.mappingStore.lookupTimelineId blobKey
      case existing of
        Just eventId -> pure (Just (BlobId eventId))
        Nothing -> do
          bytes <- downloadQQMedia context.manager context.bridge.logger mediaUrl
          case bytes of
            Nothing -> pure Nothing
            Just content -> do
              let event = Event {platformKey = blobKey, content = CreateBlob content}
              result <- submitMapped context.bridge event
              case result of
                Right eventId -> pure (Just (BlobId eventId))
                Left err -> do
                  logError context.bridge.logger ("qq media submit error: " <> Text.pack (show err))
                  pure Nothing

firstHttpUrl :: [Maybe Text] -> Maybe Text
firstHttpUrl = firstJust . map (>>= httpUrl)

httpUrl :: Text -> Maybe Text
httpUrl text
  | "http://" `Text.isPrefixOf` text = Just text
  | "https://" `Text.isPrefixOf` text = Just text
  | otherwise = Nothing

decodeQQForward :: Timeline timeline => QQContext timeline -> Int -> Value -> IO [MessagePart]
decodeQQForward context index segmentData =
  case valueTextField "id" segmentData of
    Just forwardId -> do
      let params = object ["id" .= qqIdJson forwardId, "message_id" .= qqIdJson forwardId]
      response <- qqApiCall context.bridge.logger context.manager context.config "get_forward_msg" params
      case response of
        Right value -> decodeQQNodes context (forwardPrefix context index) value
        Left err -> do
          logWarn context.bridge.logger ("qq get_forward_msg failed: " <> Text.pack err)
          pure (oneTextPart "[forward]")
    Nothing
      | hasForwardContent segmentData -> decodeQQNodes context (forwardPrefix context index) segmentData
      | otherwise -> pure (oneTextPart "[forward]")

forwardPrefix :: QQContext timeline -> Int -> Text
forwardPrefix context index = context.blobPrefix <> "/f" <> Text.pack (show index)

hasForwardContent :: Value -> Bool
hasForwardContent value = valueField "content" value /= Nothing || valueField "messages" value /= Nothing

qqNodeValues :: Value -> [Value]
qqNodeValues = \case
  Array values -> toList values
  value
    | Just (Array messages) <- valueField "messages" value -> toList messages
    | Just _ <- valueField "content" value -> [value]
    | otherwise -> []

decodeQQNodes :: Timeline timeline => QQContext timeline -> Text -> Value -> IO [MessagePart]
decodeQQNodes context prefix value = do
  messages <- mapM (uncurry (decodeQQNode context prefix)) (zip [0 ..] (qqNodeValues value))
  pure case nonEmpty (catMaybes messages) of
    Just items -> [List items]
    Nothing -> oneTextPart "[forward]"

decodeQQNode :: Timeline timeline => QQContext timeline -> Text -> Int -> Value -> IO (Maybe Message)
decodeQQNode context prefix index node = do
  let sender = fromMaybe Null (valueField "sender" node)
      name =
        firstNonempty
          [ valueTextField "card" sender
          , valueTextField "nickname" sender
          , valueTextField "name" sender
          , valueTextField "nickname" node
          , valueTextField "user_id" node
          ]
      content = fromMaybe Null (valueField "content" node)
      nodeContext = context {blobPrefix = prefix <> "/" <> Text.pack (show index)}
  (parts, _) <- decodeQQSegments nodeContext content
  pure (nonEmpty (maybe [] (\label -> oneTextPart (label <> ": ")) name <> parts))

oneInlinePart :: InlineTextPart -> [MessagePart]
oneInlinePart part = [Text (part :| [])]

qqShareInline :: Value -> InlineTextPart
qqShareInline segmentData =
  let title = fromMaybe "link" (firstNonempty [valueTextField "title" segmentData, valueTextField "content" segmentData])
   in case valueTextField "url" segmentData of
        Just url -> Link (Plain title :| []) url
        Nothing -> Plain title

qqContactText :: Value -> Text
qqContactText segmentData =
  let kind = fromMaybe "contact" (valueTextField "type" segmentData)
      target = fromMaybe "?" (valueTextField "id" segmentData)
   in "[" <> kind <> ":" <> target <> "]"

qqLocationInline :: Value -> InlineTextPart
qqLocationInline segmentData =
  case (valueTextField "lat" segmentData, valueTextField "lon" segmentData) of
    (Just lat, Just lon) ->
      let label =
            Text.intercalate
              " "
              ( filter
                  (not . Text.null)
                  [ fromMaybe "location" (valueTextField "title" segmentData)
                  , lat <> "," <> lon
                  ]
              )
          mapUrl = "https://www.openstreetmap.org/?mlat=" <> lat <> "&mlon=" <> lon <> "#map=17/" <> lat <> "/" <> lon
       in Link (Plain label :| []) mapUrl
    _ -> Plain (fromMaybe "location" (firstNonempty [valueTextField "title" segmentData, valueTextField "content" segmentData]))

qqMusicInline :: Value -> InlineTextPart
qqMusicInline segmentData =
  let title = fromMaybe "music" (firstNonempty [valueTextField "title" segmentData, valueTextField "id" segmentData])
   in case valueTextField "url" segmentData of
        Just url -> Link (Plain title :| []) url
        Nothing -> Plain title

qqJsonCardText :: Value -> Text
qqJsonCardText segmentData =
  case valueTextField "data" segmentData >>= decodeJsonText of
    Just payload -> fromMaybe "[card]" (firstCardText payload)
    Nothing -> "[card]"
 where
  firstCardText payload =
    firstNonempty
      [ valueTextField "prompt" payload
      , valueTextField "desc" payload
      , valueField "meta" payload >>= firstMetaText
      ]

  firstMetaText (Object meta) =
    firstNonempty
      [ firstNonempty [valueTextField "title" value, valueTextField "desc" value]
      | value <- toList meta
      ]
  firstMetaText _ = Nothing

qqMarkdownText :: Value -> Text
qqMarkdownText segmentData =
  fromMaybe
    "[markdown]"
    ( firstNonempty
        [ valueTextField "content" segmentData
        , valueField "data" segmentData >>= valueTextField "content"
        , valueTextField "data" segmentData >>= decodeJsonText >>= valueTextField "content"
        ]
    )

decodeJsonText :: Text -> Maybe Value
decodeJsonText raw =
  case eitherDecodeStrict' (Text.Encoding.encodeUtf8 raw) :: Either String Value of
    Right payload -> Just payload
    Left _ -> Nothing

downloadQQMedia :: Manager -> Logger -> Text -> IO (Maybe ByteString)
downloadQQMedia manager logger mediaUrl = do
  parsed <- try (parseRequest (Text.unpack mediaUrl)) :: IO (Either HttpException Request)
  case parsed of
    Left err -> logWarn logger ("qq media URL rejected: " <> Text.pack (show err)) >> pure Nothing
    Right request0 -> do
      let host = Text.toLower (Text.Encoding.decodeUtf8 request0.host)
      download 1 host request0 {redirectCount = 0}
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
          mapM_ (sendToQQRoom manager config context stored info) (filter (enabledRoom config.initialRooms) roomKeys)
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin retraction " <> renderEventId stored.storedId)
      | otherwise -> retractQQMessages manager config context stored message
    ChangeRoomNick user room nick
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin nick change " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
          mapM_ (sendQQNickChange manager config context stored user nick) (filter (enabledRoom config.initialRooms) roomKeys)
    _ -> pure ()

sendQQNickChange :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> StoredEvent -> UserId -> Text -> PlatformKey -> IO ()
sendQQNickChange manager config context stored user nick roomKey =
  case readMaybe (Text.unpack roomKey.key) :: Maybe Integer of
    Nothing -> logWarn context.logger ("qq ignoring invalid group id " <> roomKey.key)
    Just groupId -> do
      alreadySent <- qqMessageMappedToGroup context stored.storedId roomKey.key
      unless alreadySent do
        text <- renderNickChange context user nick
        let params = object ["group_id" .= groupId, "message" .= [qqTextSegment text.plainText]]
        response <- qqApiCall context.logger manager config "send_group_msg" params
        case response of
          Left err -> logError context.logger ("qq nick send_group_msg failed: " <> Text.pack err)
          Right result ->
            case valueTextField "message_id" result of
              Nothing -> logError context.logger "qq nick send_group_msg response lacked message_id"
              Just messageId ->
                context.mappingStore.insertMapping (qqMessageKey roomKey.key messageId) stored.storedId

sendToQQRoom :: Timeline timeline => Manager -> QQConfig -> BridgeContext timeline -> StoredEvent -> SendMessageInfo -> PlatformKey -> IO ()
sendToQQRoom manager config context stored info roomKey =
  case readMaybe (Text.unpack roomKey.key) :: Maybe Integer of
    Nothing -> logWarn context.logger ("qq ignoring invalid group id " <> roomKey.key)
    Just groupId -> do
      alreadySent <- qqMessageMappedToGroup context stored.storedId roomKey.key
      unless alreadySent do
        let qqContext =
              QQContext
                { manager
                , config
                , bridge = context
                , room = info.room
                , groupId = roomKey.key
                , blobPrefix = ""
                }
        body <- renderQQMessage qqContext info
        replyTo <- qqReplyMessageId context roomKey.key info.replyTo
        let segments = maybe body (\messageId -> qqReplySegment messageId : body) replyTo
            params = object ["group_id" .= groupId, "message" .= segments]
        response <- qqApiCall context.logger manager config "send_group_msg" params
        case response of
          Left err -> logError context.logger ("qq send_group_msg failed: " <> Text.pack err)
          Right result ->
            case valueTextField "message_id" result of
              Nothing -> logError context.logger "qq send_group_msg response lacked message_id"
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
      mapM_ (deleteQQMessage manager config context stored) (filter (enabledRoom config.initialRooms . qqRoomKey . fst) (mapMaybe qqMessageTarget keys))
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

renderQQMessage :: Timeline timeline => QQContext timeline -> SendMessageInfo -> IO [Value]
renderQQMessage context info = do
  senderInfo <- getUserInfo context.bridge.timeline info.sender (Just info.room)
  let senderName = maybe "unknown" (.name) senderInfo
  body <- renderQQMessageBody context info.body
  pure (qqTextSegment (senderName <> ": ") : body)

renderQQMessageBody :: Timeline timeline => QQContext timeline -> Message -> IO [Value]
renderQQMessageBody context = fmap concat . mapM (renderQQMessagePart context) . toList

renderQQMessagePart :: Timeline timeline => QQContext timeline -> MessagePart -> IO [Value]
renderQQMessagePart context = \case
  Text inline -> renderQQInline context inline
  Emote inline -> (qqTextSegment "* " :) <$> renderQQInline context inline
  Blob blobType blobId -> renderQQBlob context blobType blobId
  List messages -> concat <$> mapM (\message -> (qqTextSegment "- " :) <$> renderQQMessageBody context message) (toList messages)
  BlockQuote message -> (qqTextSegment "> " :) <$> renderQQMessageBody context message
  Heading level inline ->
    let safeLevel = max 1 (min 6 level)
     in (qqTextSegment (Text.replicate safeLevel "#" <> " ") :) <$> renderQQInline context inline

renderQQInline :: Timeline timeline => QQContext timeline -> InlineText -> IO [Value]
renderQQInline context = fmap concat . mapM (renderQQInlinePart context) . toList

renderQQInlinePart :: Timeline timeline => QQContext timeline -> InlineTextPart -> IO [Value]
renderQQInlinePart context = \case
  Bold inline -> wrapInline "*" inline
  Italic inline -> wrapInline "_" inline
  Link inline url -> do
    rendered <- renderQQInline context inline
    pure (rendered <> [qqTextSegment " (", qqTextSegment url, qqTextSegment ")"])
  Mention inline (Left user) -> do
    keys <- context.bridge.mappingStore.lookupPlatformKeys (userEventId user)
    case [key.key | key <- keys, key.platform == "qq", key.entityType == "user"] of
      qqUserId : _ -> pure [object ["type" .= ("at" :: Text), "data" .= object ["qq" .= qqIdJson qqUserId]]]
      [] -> renderQQInline context inline
  Mention inline (Right mentionedRoom)
    | roomEventId mentionedRoom == roomEventId context.room ->
        pure [object ["type" .= ("at" :: Text), "data" .= object ["qq" .= ("all" :: Text)]]]
    | otherwise -> renderQQInline context inline
  InlineQuote inline -> do
    rendered <- renderQQInline context inline
    pure ([qqTextSegment "\""] <> rendered <> [qqTextSegment "\""])
  Plain text -> pure [qqTextSegment text]
 where
  wrapInline marker inline = do
    rendered <- renderQQInline context inline
    pure ([qqTextSegment marker] <> rendered <> [qqTextSegment marker])

renderQQBlob :: Timeline timeline => QQContext timeline -> BlobType -> BlobId -> IO [Value]
renderQQBlob context blobType blobId = do
  stored <- getEvent context.bridge.timeline (blobEventId blobId)
  case fmap (.storedEvent.content) stored of
    Just (CreateBlob bytes) ->
      case blobType of
        FileBlob -> uploadQQFile context blobId bytes
        _ -> pure [qqMediaSegment blobType bytes]
    _ -> pure [qqTextSegment "[blob]"]

qqMediaSegment :: BlobType -> ByteString -> Value
qqMediaSegment blobType bytes =
  let kind = case blobType of
        ImageBlob -> "image"
        VideoBlob -> "video"
        AudioBlob -> "record"
        FileBlob -> "file"
      file = "base64://" <> Text.Encoding.decodeUtf8 (Base64.encode bytes)
   in object ["type" .= (kind :: Text), "data" .= object ["file" .= file]]

uploadQQFile :: Timeline timeline => QQContext timeline -> BlobId -> ByteString -> IO [Value]
uploadQQFile context blobId bytes = do
  let name = "file-" <> Text.take 12 (renderEventId (blobEventId blobId)) <> sniffExtension bytes
      params =
        object
          [ "group_id" .= qqIdJson context.groupId
          , "file" .= ("base64://" <> Text.Encoding.decodeUtf8 (Base64.encode bytes))
          , "name" .= name
          ]
  response <- qqApiCall context.bridge.logger context.manager context.config "upload_group_file" params
  case response of
    Right _ -> pure [qqTextSegment ("[" <> name <> "]")]
    Left err -> do
      logError context.bridge.logger ("qq upload_group_file failed: " <> Text.pack err)
      pure [qqTextSegment "[file]"]

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
  , qqTime :: Maybe Text
  , qqCardNew :: Maybe Text
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
        , qqTime = valueTextField "time" (Object obj)
        , qqCardNew = valueTextField "card_new" (Object obj)
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
  String text -> parseCQSegments text
  _ -> []

-- | OneBot implementations may deliver messages as CQ code strings instead of
-- segment arrays. Decode those into the same segment shape.
parseCQSegments :: Text -> [Value]
parseCQSegments = go
 where
  go text =
    case Text.breakOn "[CQ:" text of
      (plain, rest)
        | Text.null rest -> textSegments plain
        | otherwise ->
            textSegments plain <> case Text.breakOn "]" rest of
              (rawSegment, remainder)
                | Text.null remainder -> [cqSegment rawSegment]
                | otherwise -> cqSegment rawSegment : go (Text.drop 1 remainder)

  textSegments plain = [qqTextSegment (unescapeCQ plain) | not (Text.null plain)]

  cqSegment rawSegment =
    let body = Text.drop 4 rawSegment
        (kind, parameterText) = Text.breakOn "," body
     in object
          [ "type" .= kind
          , "data" .= object [Aeson.Key.fromText key .= value | (key, value) <- cqParameters parameterText]
          ]

  cqParameters parameterText =
    [ (key, unescapeCQ rawValue)
    | pair <- Text.splitOn "," (Text.drop 1 parameterText)
    , let (key, rawValue) = Text.breakOn "=" pair
    , not (Text.null key)
    ]

unescapeCQ :: Text -> Text
unescapeCQ =
  Text.replace "&amp;" "&"
    . Text.replace "&#44;" ","
    . Text.replace "&#93;" "]"
    . Text.replace "&#91;" "["


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

qqBlobKey :: Text -> Int -> PlatformKey
qqBlobKey prefix index = PlatformKey "qq" "blob" (prefix <> "/" <> Text.pack (show index))

qqRetractionKey :: Text -> EventId -> PlatformKey
qqRetractionKey groupId eventId_ = PlatformKey "qq" "retraction" (groupId <> "/" <> renderEventId eventId_)

messageGroupId :: PlatformKey -> Maybe Text
messageGroupId key =
  case Text.splitOn "/" key.key of
    groupId : _ -> Just groupId
    [] -> Nothing

renderEventId :: EventId -> Text
renderEventId (EventId eventId_) = eventId_
