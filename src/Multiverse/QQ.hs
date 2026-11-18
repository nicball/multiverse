module Multiverse.QQ
  ( qqBridge
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
import Data.Maybe (catMaybes, fromMaybe, listToMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types.Status (statusCode)
import Network.WebSockets qualified as WebSockets
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Config (InitialRoomMapping (..), QQBridgeConfig (..))
import Multiverse.Event hiding (eventId)
import Multiverse.HTTP (HttpLogOptions (..), defaultHttpLogOptions)
import Multiverse.HTTP qualified as HTTP
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data Context timeline = Context
  { timeline :: timeline
  , mappingStore :: MappingStore
  , logger :: Logger
  , manager :: Manager
  , config :: QQBridgeConfig
  , endpoint :: QQEndpoint
  }

data QQEndpoint = QQEndpoint
  { host :: String
  , port :: Int
  , path :: String
  , headers :: WebSockets.Headers
  }

qqBridge :: Timeline timeline => BridgeRuntime timeline -> QQBridgeConfig -> IO (Bridge, QQBridgeConfig)
qqBridge runtime initialConfig = do
  mappingStore <- openMappingStore initialConfig.mappingDb
  manager <- newManager tlsManagerSettings
  (host, port, path) <- parseWebSocketUrl initialConfig.websocketUrl
  let headers =
        if Text.null initialConfig.accessToken
          then []
          else [("Authorization", "Bearer " <> Text.Encoding.encodeUtf8 initialConfig.accessToken)]
      provisionalContext =
        Context
          { timeline = runtime.timeline
          , mappingStore
          , logger = runtime.logger
          , manager
          , config = initialConfig
          , endpoint = QQEndpoint {host, port, path, headers}
          }
  initialRooms <-
    ensureInitialRooms
      runtime.logger
      runtime.timeline
      mappingStore
      (fetchInitialRoomInfo provisionalContext)
      initialConfig.initialRooms
  let config = initialConfig {initialRooms}
      context = provisionalContext {config}
  logInfo runtime.logger ("qq bridge started with mapping db " <> Text.pack config.mappingDb)
  pure
    ( Bridge
        { bridgeName = "qq"
        , observe = observeQQ context
        , reflect = reflectQQ context
        }
    , config
    )

observeQQ :: Timeline timeline => Context timeline -> IO ()
observeQQ context =
  forever do
    result <- try (runQQWebSocket context) :: IO (Either SomeException ())
    case result of
      Left err -> logError context.logger ("qq observer disconnected: " <> Text.pack (show err))
      Right () -> logWarn context.logger "qq observer websocket closed"
    threadDelay 5000000

runQQWebSocket :: Timeline timeline => Context timeline -> IO ()
runQQWebSocket context =
  WebSockets.runClientWith context.endpoint.host context.endpoint.port context.endpoint.path WebSockets.defaultConnectionOptions context.endpoint.headers \connection -> do
    logInfo context.logger "qq observer connected"
    forever do
      payload <- WebSockets.receiveData connection :: IO LByteString.ByteString
      case eitherDecode payload of
        Left err -> logWarn context.logger ("qq observer ignored invalid event: " <> Text.pack err)
        Right value ->
          case parseMaybe parseJSON value of
            Nothing -> logDebug context.logger "qq observer ignored unrecognized event"
            Just event -> observeQQEvent context event

parseWebSocketUrl :: Text -> IO (String, Int, String)
parseWebSocketUrl url
  | not ("ws://" `Text.isPrefixOf` url) = fail "qq websocket_url must use ws://"
  | otherwise = do
      request <- parseRequest ("http://" <> Text.unpack (Text.drop 5 url))
      let host = Text.Encoding.decodeUtf8 request.host
          path = Text.Encoding.decodeUtf8 request.path <> Text.Encoding.decodeUtf8 request.queryString
      pure (Text.unpack host, request.port, Text.unpack path)

observeQQEvent :: Timeline timeline => Context timeline -> QQEvent -> IO ()
observeQQEvent context event =
  case (event.qqPostType, event.qqNoticeType, event.qqMessageType) of
    (Just "message", _, Just "group") -> observeQQMessage context event
    (Just "message", _, Just "private") -> logDebug context.logger "qq ignoring private message"
    (Just "message_sent", _, _) -> logDebug context.logger "qq ignoring echoed message"
    (Just "notice", Just "group_recall", _) -> observeQQRetraction context event
    (Just "notice", Just "group_increase", _) -> observeQQMembership context event True
    (Just "notice", Just "group_decrease", _) -> observeQQMembership context event False
    (Just "notice", Just "group_kick", _) -> observeQQMembership context event False
    (Just "notice", Just "group_card", _) -> observeQQCard context event
    (Just postType, Just noticeType, _) ->
      logDebug context.logger ("qq ignoring notice " <> postType <> "/" <> noticeType)
    _ -> logDebug context.logger "qq ignoring unrecognized event"

observeQQMessage :: Timeline timeline => Context timeline -> QQEvent -> IO ()
observeQQMessage context event =
  case (event.qqGroupId, event.qqUserId, event.qqMessageId) of
    (Just groupId, Just userId, Just messageId)
      | event.qqSelfId == Just userId ->
          logDebug context.logger ("qq ignoring own message " <> messageId)
      | otherwise -> do
          room <- lookupConfiguredRoom context.mappingStore context.config.initialRooms (qqRoomKey groupId)
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
                  ensureRoomMembership context.logger context.timeline context.mappingStore "qq" roomId sender
                  let messageContext =
                        QQContext
                          { bridge = context
                          , room = roomId
                          , groupId
                          , blobPrefix = messageKey.key
                          , forwardRoot = Just messageId
                          , forwardDepth = 0
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
                  submitted <- submitMessageMapped context.logger context.timeline context.mappingStore timelineEvent
                  case submitted of
                    Right _ -> pure ()
                    Left err -> logError context.logger ("qq message submit error: " <> Text.pack (show err))
    _ -> logDebug context.logger "qq ignored group message with incomplete identifiers"

observeQQMembership :: Timeline timeline => Context timeline -> QQEvent -> Bool -> IO ()
observeQQMembership context event joined =
  case (event.qqGroupId, event.qqUserId) of
    (Just groupId, Just userId)
      | event.qqSelfId == Just userId ->
          logDebug context.logger ("qq ignoring own membership " <> userId)
      | otherwise -> do
          room <- lookupConfiguredRoom context.mappingStore context.config.initialRooms (qqRoomKey groupId)
          case room of
            Nothing -> pure ()
            Just roomId -> do
              name <- qqGroupMemberName context groupId userId (qqSenderName userId event.qqSender)
              user <- ensureQQUser context userId name
              previous <- getMembership context.timeline user roomId
              let key = PlatformKey "qq" (if joined then "join" else "leave") (noticeKey groupId userId event.qqTime)
                  content = if joined then JoinRoom user roomId previous else LeaveRoom user roomId previous
              result <- submitMapped context.logger context.timeline context.mappingStore Event {platformKey = key, content}
              case result of
                Left err -> logError context.logger ("qq membership submit error: " <> Text.pack (show err))
                Right _ -> pure ()
    _ -> logDebug context.logger "qq ignored membership notice with incomplete identifiers"

observeQQCard :: Timeline timeline => Context timeline -> QQEvent -> IO ()
observeQQCard context event =
  case (event.qqGroupId, event.qqUserId, event.qqCardNew) of
    (Just groupId, Just userId, Just card)
      | event.qqSelfId == Just userId ->
          logDebug context.logger ("qq ignoring own nickname change " <> userId)
      | not (Text.null card) -> do
          room <- lookupConfiguredRoom context.mappingStore context.config.initialRooms (qqRoomKey groupId)
          case room of
            Nothing -> pure ()
            Just roomId -> do
              let senderName = qqSenderName userId event.qqSender
              user <- ensureQQUser context userId (if senderName == userId then card else senderName)
              let key = PlatformKey "qq" "nick" (noticeKey groupId userId event.qqTime)
              result <- submitMapped context.logger context.timeline context.mappingStore Event {platformKey = key, content = ChangeRoomNick user roomId card}
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
qqGroupMemberName :: Context timeline -> Text -> Text -> Text -> IO Text
qqGroupMemberName context groupId userId fallback
  | fallback /= userId = pure fallback
  | otherwise = do
      response <-
        qqApiCall
          context
          "get_group_member_info"
          (object ["group_id" .= qqIdJson groupId, "user_id" .= qqIdJson userId])
      case response of
        Left err -> do
          logDebug context.logger ("qq get_group_member_info failed: " <> Text.pack err)
          pure fallback
        Right info ->
          pure (fromMaybe fallback (firstNonempty [valueTextField "card" info, valueTextField "nickname" info]))

observeQQRetraction :: Timeline timeline => Context timeline -> QQEvent -> IO ()
observeQQRetraction context event =
  case (event.qqGroupId, event.qqMessageId) of
    (Just groupId, Just targetMessageId) -> do
      room <- lookupConfiguredRoom context.mappingStore context.config.initialRooms (qqRoomKey groupId)
      case room of
        Nothing -> pure ()
        Just _ -> do
          target <- context.mappingStore.lookupTimelineId (qqMessageKey groupId targetMessageId)
          case target of
            Nothing -> logDebug context.logger ("qq ignoring recall for unknown message " <> targetMessageId)
            Just targetId -> do
              let key = PlatformKey "qq" "retraction" (groupId <> "/" <> targetMessageId)
                  eventToSubmit = Event {platformKey = key, content = RetractMessage (MessageId targetId)}
              result <- submitMapped context.logger context.timeline context.mappingStore eventToSubmit
              case result of
                Left err -> logError context.logger ("qq retraction submit error: " <> Text.pack (show err))
                Right _ -> pure ()
    _ -> logDebug context.logger "qq ignored recall event with incomplete identifiers"

qqReplyTo :: Context timeline -> Text -> Maybe Text -> IO (Maybe MessageId)
qqReplyTo _ _ Nothing = pure Nothing
qqReplyTo context groupId (Just messageId) = do
  mapped <- context.mappingStore.lookupTimelineId (qqMessageKey groupId messageId)
  pure (MessageId <$> mapped)

ensureQQUser :: Timeline timeline => Context timeline -> Text -> Text -> IO UserId
ensureQQUser context qqUserId displayName = do
  let name = if Text.null displayName then qqUserId else displayName
  mapped <- ensureUserMapped context.logger context.timeline context.mappingStore (qqUserKey qqUserId) (pure name)
  either (\err -> fail ("could not create qq user: " <> show err)) pure mapped

data QQContext timeline = QQContext
  { bridge :: Context timeline
  , room :: RoomId
  , groupId :: Text
  , blobPrefix :: Text
  , forwardRoot :: Maybe Text
  , forwardDepth :: Int
  }

-- | QQ never expands a forward nested inside another forward, so nesting is
-- only worth a bounded amount of work.
maxForwardDepth :: Int
maxForwardDepth = 3

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
      blob <- importQQSegmentBlob context index (segmentMediaUrl segmentData)
      let flash = valueTextField "type" segmentData == Just "flash"
          parts = qqBlobParts ImageBlob blob "image"
      pure ((if flash then oneTextPart "[flash]" else []) <> parts, Nothing)
    Just "mface" -> do
      blob <- importQQSegmentBlob context index (mfaceUrl segmentData)
      let label = fromMaybe "mface" (valueTextField "summary" segmentData)
      pure (qqBlobParts ImageBlob blob label, Nothing)
    Just "record" -> decodeQQTypedBlob context index AudioBlob "record" segmentData
    Just "video" -> decodeQQTypedBlob context index VideoBlob "video" segmentData
    Just "cardimage" -> decodeQQTypedBlob context index ImageBlob "cardimage" segmentData
    Just "file" -> do
      blob <- importQQSegmentBlob context index (segmentMediaUrl segmentData)
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
    Just "node"
      | context.forwardDepth >= maxForwardDepth -> pure (oneTextPart "[forward]", Nothing)
      | otherwise -> (, Nothing) <$> decodeQQNodes context (forwardPrefix context index) segmentData
    Just other -> do
      logDebug context.bridge.logger ("qq ignoring unsupported segment type " <> other)
      pure (oneTextPart ("[" <> other <> "]"), Nothing)

decodeQQTypedBlob :: Timeline timeline => QQContext timeline -> Int -> BlobType -> Text -> Value -> IO ([MessagePart], Maybe Text)
decodeQQTypedBlob context index blobType label segmentData = do
  blob <- importQQSegmentBlob context index (segmentMediaUrl segmentData)
  pure (qqBlobParts blobType blob label, Nothing)

qqBlobParts :: BlobType -> Maybe BlobId -> Text -> [MessagePart]
qqBlobParts blobType (Just blobId) _ = [Blob blobType blobId]
qqBlobParts _ Nothing label = oneTextPart ("[" <> label <> "]")

decodeQQMention :: Timeline timeline => QQContext timeline -> Value -> IO [MessagePart]
decodeQQMention context segmentData =
  case valueTextField "qq" segmentData of
    Just "all" -> pure (oneInlinePart (Mention (plainInline "@all") (Right context.room)))
    Just userId -> do
      let name = fromMaybe userId (firstNonempty [valueTextField "name" segmentData])
      user <- ensureQQUser context.bridge userId name
      pure (oneInlinePart (Mention (plainInline ("@" <> name)) (Left user)))
    Nothing -> pure (oneTextPart "[mention]")

importQQSegmentBlob :: Timeline timeline => QQContext timeline -> Int -> Maybe Text -> IO (Maybe BlobId)
importQQSegmentBlob context index mediaUrl =
  case mediaUrl of
    Nothing -> pure Nothing
    Just url -> do
      let blobKey = qqBlobKey context.blobPrefix index
      existing <- context.bridge.mappingStore.lookupTimelineId blobKey
      case existing of
        Just eventId -> pure (Just (BlobId eventId))
        Nothing -> do
          bytes <- downloadQQMedia context.bridge url
          case bytes of
            Nothing -> pure Nothing
            Just content -> do
              result <- submitBlob context.bridge.logger context.bridge.timeline context.bridge.mappingStore blobKey content
              case result of
                Right blob -> pure (Just blob)
                Left err -> do
                  logError context.bridge.logger ("qq media submit error: " <> Text.pack (show err))
                  pure Nothing

segmentMediaUrl :: Value -> Maybe Text
segmentMediaUrl segmentData =
  firstHttpUrl
    [ valueTextField "url" segmentData
    , valueTextField "file" segmentData
    , valueTextField "path" segmentData
    ]

-- | Market stickers usually arrive without a url, but the emoji id addresses
-- the animated asset on the QQ sticker CDN directly.
mfaceUrl :: Value -> Maybe Text
mfaceUrl segmentData =
  case segmentMediaUrl segmentData of
    Just url -> Just url
    Nothing -> case valueTextField "emoji_id" segmentData of
      Just emojiId
        | Text.length emojiId > 2 ->
            Just
              ( "https://gxh.vip.qq.com/club/item/parcel/item/"
                  <> Text.take 2 emojiId
                  <> "/"
                  <> emojiId
                  <> "/raw300.gif"
              )
      _ -> Nothing

firstHttpUrl :: [Maybe Text] -> Maybe Text
firstHttpUrl = firstJust . map (>>= httpUrl)

httpUrl :: Text -> Maybe Text
httpUrl text
  | "http://" `Text.isPrefixOf` text = Just text
  | "https://" `Text.isPrefixOf` text = Just text
  | otherwise = Nothing

decodeQQForward :: Timeline timeline => QQContext timeline -> Int -> Value -> IO [MessagePart]
decodeQQForward context index segmentData = do
  resolved <- resolveForwardContent context segmentData
  case resolved of
    Just value -> decodeQQNodes context (forwardPrefix context index) value
    Nothing -> pure (oneTextPart "[forward]")

-- | A @forward@ segment carries the resolved nodes only when NapCat's OneBot
-- @parseMultMsg@ option is on, and @get_forward_msg@ cannot expand a forward
-- nested inside another one ("消息已过期或者为内层消息"). Content already in
-- hand therefore wins, then the container message, then the API call.
resolveForwardContent :: Timeline timeline => QQContext timeline -> Value -> IO (Maybe Value)
resolveForwardContent context segmentData
  | hasForwardContent segmentData = pure (Just segmentData)
  | context.forwardDepth >= maxForwardDepth = pure Nothing
  | otherwise = do
      fromMessage <- forwardContentFromMessage context
      case fromMessage of
        Just value -> pure (Just value)
        Nothing -> forwardContentFromApi context segmentData

-- | A freshly observed message often still has its forward card expanded, and
-- @get_msg@ hands the card body back inline even when the event did not.
forwardContentFromMessage :: Timeline timeline => QQContext timeline -> IO (Maybe Value)
forwardContentFromMessage context =
  case context.forwardRoot of
    Just messageId
      | context.forwardDepth == 0 -> do
          response <- qqApiCall context.bridge "get_msg" (object ["message_id" .= qqIdJson messageId])
          case response of
            Right value ->
              pure
                ( firstJust
                    (map inlineForwardContent (messageSegments (fromMaybe Null (valueField "message" value))))
                )
            Left err -> do
              reportForwardFailure context "get_msg" err
              pure Nothing
    _ -> pure Nothing

inlineForwardContent :: Value -> Maybe Value
inlineForwardContent segment
  | valueTextField "type" segment == Just "forward"
  , Just segmentData <- valueField "data" segment
  , hasForwardContent segmentData =
      Just segmentData
  | otherwise = Nothing

forwardContentFromApi :: Timeline timeline => QQContext timeline -> Value -> IO (Maybe Value)
forwardContentFromApi context segmentData =
  case forwardIdOf segmentData of
    Nothing -> pure Nothing
    Just forwardId -> do
      let params =
            object
              [ "id" .= qqIdJson forwardId
              , "message_id" .= qqIdJson forwardId
              ]
      response <- qqApiCall context.bridge "get_forward_msg" params
      case response of
        Right value -> pure (Just value)
        Left err -> do
          reportForwardFailure context "get_forward_msg" err
          pure Nothing

-- | NapCat names the forward resource @id@, other OneBot builds @res_id@.
forwardIdOf :: Value -> Maybe Text
forwardIdOf segmentData =
  firstJust
    [ valueTextField "id" segmentData
    , valueTextField "res_id" segmentData
    , valueTextField "resId" segmentData
    ]

-- | An expired forward and a nested one are normal traffic, not incidents.
reportForwardFailure :: Timeline timeline => QQContext timeline -> Text -> String -> IO ()
reportForwardFailure context action err =
  let message = "qq " <> action <> " could not expand forward: " <> Text.pack err
      expected marker = marker `Text.isInfixOf` Text.pack err
   in if any expected ["已过期", "内层消息"]
        then logDebug context.bridge.logger message
        else logWarn context.bridge.logger message

forwardPrefix :: QQContext timeline -> Int -> Text
forwardPrefix context index = context.blobPrefix <> "/f" <> Text.pack (show index)

hasForwardContent :: Value -> Bool
hasForwardContent = not . null . qqNodeValues

-- | Node lists arrive in several shapes: the OneBot @messages@ array, the
-- inline @content@ array NapCat emits for expanded forwards, a bare array, or
-- a single node on its own.
qqNodeValues :: Value -> [Value]
qqNodeValues = \case
  Array values -> toList values
  value@(Object _)
    | Just (Array messages) <- valueField "messages" value -> toList messages
    | Just (Array nodes) <- valueField "content" value
    , not (null (toList nodes))
    , all looksLikeNode (toList nodes) ->
        toList nodes
    | looksLikeNode value -> [value]
    | Just _ <- valueField "content" value -> [value]
    | otherwise -> []
  _ -> []

-- | NapCat's @get_forward_msg@ returns flat nodes, while OneBot node segments
-- wrap a node in @data@.
looksLikeNode :: Value -> Bool
looksLikeNode value@(Object _) = any (\key -> valueField key value /= Nothing) nodeMarkerKeys
looksLikeNode _ = False

nodeMarkerKeys :: [Text]
nodeMarkerKeys =
  [ "sender"
  , "user_id"
  , "uin"
  , "nickname"
  , "raw_message"
  , "message_seq"
  , "real_id"
  , "message_type"
  , "message"
  ]

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
          , valueTextField "name" node
          , valueTextField "uin" node
          , valueTextField "user_id" node
          ]
      nodeContext =
        context
          { blobPrefix = prefix <> "/" <> Text.pack (show index)
          , forwardDepth = context.forwardDepth + 1
          }
  (parts, _) <- decodeQQSegments nodeContext (nodeBodyOf node)
  pure (nonEmpty (maybe [] (\label -> oneTextPart (label <> ": ")) name <> parts))

-- | NapCat flat nodes keep the body in @message@, OneBot node segments in
-- @content@, and @raw_message@ is the text of last resort.
nodeBodyOf :: Value -> Value
nodeBodyOf node
  | not (null (messageSegments messageBody)) = messageBody
  | not (null (messageSegments contentBody)) = contentBody
  | otherwise = maybe Null String rawText
 where
  messageBody = fromMaybe Null (valueField "message" node)
  contentBody = fromMaybe Null (valueField "content" node)
  rawText = valueTextField "raw_message" node

oneInlinePart :: InlineTextPart -> [MessagePart]
oneInlinePart part = [Text (part :| [])]

qqShareInline :: Value -> InlineTextPart
qqShareInline segmentData =
  let title = fromMaybe "link" (firstNonempty [valueTextField "title" segmentData, valueTextField "content" segmentData])
   in case valueTextField "url" segmentData of
        Just url -> Link (plainInline title) url
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
       in Link (plainInline label) mapUrl
    _ -> Plain (fromMaybe "location" (firstNonempty [valueTextField "title" segmentData, valueTextField "content" segmentData]))

qqMusicInline :: Value -> InlineTextPart
qqMusicInline segmentData =
  let title = fromMaybe "music" (firstNonempty [valueTextField "title" segmentData, valueTextField "id" segmentData])
   in case valueTextField "url" segmentData of
        Just url -> Link (plainInline title) url
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

downloadQQMedia :: Context timeline -> Text -> IO (Maybe ByteString)
downloadQQMedia context mediaUrl = do
  parsed <- try (parseRequest (Text.unpack mediaUrl)) :: IO (Either HttpException Request)
  case parsed of
    Left err -> logWarn context.logger ("qq media URL rejected: " <> Text.pack (show err)) >> pure Nothing
    Right request0 -> do
      let host = Text.toLower (Text.Encoding.decodeUtf8 request0.host)
      download 1 host request0 {redirectCount = 0}
 where
  download attempt host request = do
    logDebug context.logger ("qq media GET host=" <> host)
    result <- try (httpLbs request context.manager) :: IO (Either HttpException (Response LByteString.ByteString))
    case result of
      Left err
        | attempt < 3 -> do
            logWarn context.logger ("qq media request failed; retrying: " <> Text.pack (show err))
            threadDelay (attempt * 1000000)
            download (attempt + 1) host request
        | otherwise -> logError context.logger ("qq media request failed: " <> Text.pack (show err)) >> pure Nothing
      Right response -> do
        let bytes = response.responseBody
            size = LByteString.length bytes
            status = statusCode response.responseStatus
        logDebug context.logger ("qq media response status=" <> Text.pack (show status) <> " bytes=" <> Text.pack (show size))
        if status >= 200 && status < 300 && size <= 8000000
          then pure (Just (LByteString.toStrict bytes))
          else do
            logWarn context.logger "qq media response rejected (status or size limit)"
            pure Nothing

reflectQQ :: Timeline timeline => Context timeline -> IO ()
reflectQQ context =
  forever do
    afterEventId <- fmap (fmap EventId) (context.mappingStore.lookupState "qq.reflect_event")
    storedEvents <- getEventsAfter context.timeline afterEventId
    mapM_ (reflectQQEvent context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "qq.reflect_event" (renderEventId ((last storedEvents).storedId))

reflectQQEvent :: Timeline timeline => Context timeline -> StoredEvent -> IO ()
reflectQQEvent context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
          mapM_ (sendToQQRoom context stored info) (filter (enabledRoom context.config.initialRooms) roomKeys)
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin retraction " <> renderEventId stored.storedId)
      | otherwise -> retractQQMessages context stored message
    ChangeRoomNick user room nick
      | stored.storedEvent.platformKey.platform == "qq" ->
          logDebug context.logger ("qq not reflecting own-origin nick change " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
          mapM_ (sendQQNickChange context stored user nick) (filter (enabledRoom context.config.initialRooms) roomKeys)
    _ -> pure ()

sendQQNickChange :: Timeline timeline => Context timeline -> StoredEvent -> UserId -> Text -> PlatformKey -> IO ()
sendQQNickChange context stored user nick roomKey =
  case readMaybe (Text.unpack roomKey.key) :: Maybe Integer of
    Nothing -> logWarn context.logger ("qq ignoring invalid group id " <> roomKey.key)
    Just groupId -> do
      alreadySent <- qqMessageMappedToGroup context stored.storedId roomKey.key
      unless alreadySent do
        text <- renderNickChange context.timeline user nick
        let params = object ["group_id" .= groupId, "message" .= [qqTextSegment text.plainText]]
        response <- qqApiCall context "send_group_msg" params
        case response of
          Left err -> logError context.logger ("qq nick send_group_msg failed: " <> Text.pack err)
          Right result ->
            case valueTextField "message_id" result of
              Nothing -> logError context.logger "qq nick send_group_msg response lacked message_id"
              Just messageId -> do
                let platformKey = qqMessageKey roomKey.key messageId
                context.mappingStore.insertMapping platformKey stored.storedId
                logReflected context.logger stored platformKey

sendToQQRoom :: Timeline timeline => Context timeline -> StoredEvent -> SendMessageInfo -> PlatformKey -> IO ()
sendToQQRoom context stored info roomKey =
  case readMaybe (Text.unpack roomKey.key) :: Maybe Integer of
    Nothing -> logWarn context.logger ("qq ignoring invalid group id " <> roomKey.key)
    Just groupId -> do
      alreadySent <- qqMessageMappedToGroup context stored.storedId roomKey.key
      unless alreadySent do
        let qqContext =
              QQContext
                { bridge = context
                , room = info.room
                , groupId = roomKey.key
                , blobPrefix = ""
                , forwardRoot = Nothing
                , forwardDepth = 0
                }
        body <- renderQQMessage qqContext info
        replyTo <- qqReplyMessageId context roomKey.key info.replyTo
        let segments = maybe body (\messageId -> qqReplySegment messageId : body) replyTo
            params = object ["group_id" .= groupId, "message" .= segments]
        response <- qqApiCall context "send_group_msg" params
        case response of
          Left err -> logError context.logger ("qq send_group_msg failed: " <> Text.pack err)
          Right result ->
            case valueTextField "message_id" result of
              Nothing -> logError context.logger "qq send_group_msg response lacked message_id"
              Just messageId -> do
                let platformKey = qqMessageKey roomKey.key messageId
                context.mappingStore.insertMapping platformKey stored.storedId
                logReflected context.logger stored platformKey

qqMessageMappedToGroup :: Context timeline -> EventId -> Text -> IO Bool
qqMessageMappedToGroup context eventId_ groupId = do
  keys <- lookupMappedKeys context.mappingStore "qq" "message" eventId_
  pure (any ((== Just groupId) . messageGroupId) keys)

qqReplyMessageId :: Context timeline -> Text -> Maybe MessageId -> IO (Maybe Text)
qqReplyMessageId _ _ Nothing = pure Nothing
qqReplyMessageId context groupId (Just message) = do
  keys <- lookupMappedKeys context.mappingStore "qq" "message" (messageEventId message)
  pure (firstJust (map (qqMessageIdForGroup groupId) keys))

qqMessageIdForGroup :: Text -> Text -> Maybe Text
qqMessageIdForGroup groupId key =
  case Text.splitOn "/" key of
    [mappedGroupId, messageId]
      | mappedGroupId == groupId -> Just messageId
    _ -> Nothing

retractQQMessages :: Timeline timeline => Context timeline -> StoredEvent -> MessageId -> IO ()
retractQQMessages context stored message = do
  original <- lookupSendMessage context.timeline message
  case original of
    Just _ -> do
      keys <- lookupMappedKeys context.mappingStore "qq" "message" (messageEventId message)
      mapM_ (deleteQQMessage context stored) (filter (enabledRoom context.config.initialRooms . qqRoomKey . fst) (mapMaybe qqMessageTarget keys))
    Nothing -> pure ()

qqMessageTarget :: Text -> Maybe (Text, Text)
qqMessageTarget key =
  case Text.splitOn "/" key of
    [groupId, messageId] -> Just (groupId, messageId)
    _ -> Nothing

deleteQQMessage :: Context timeline -> StoredEvent -> (Text, Text) -> IO ()
deleteQQMessage context stored (groupId, messageId) = do
  let key = qqRetractionKey groupId stored.storedId
  alreadyDeleted <- context.mappingStore.lookupTimelineId key
  case alreadyDeleted of
    Just _ -> pure ()
    Nothing -> do
      response <- qqApiCall context "delete_msg" (object ["message_id" .= qqIdJson messageId])
      case response of
        Left err -> logError context.logger ("qq delete_msg failed: " <> Text.pack err)
        Right _ -> do
          context.mappingStore.insertMapping key stored.storedId
          logReflected context.logger stored key

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
    keys <- lookupMappedKeys context.bridge.mappingStore "qq" "user" (userEventId user)
    case keys of
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
  response <- qqApiCall context.bridge "upload_group_file" params
  case response of
    Right _ -> pure [qqTextSegment ("[" <> name <> "]")]
    Left err -> do
      logError context.bridge.logger ("qq upload_group_file failed: " <> Text.pack err)
      pure [qqTextSegment "[file]"]

fetchInitialRoomInfo :: Context timeline -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo context mapping = do
  response <- qqApiCall context "get_group_info" (object ["group_id" .= qqIdJson mapping.platformKey.key])
  case response of
    Left err -> fail ("qq get_group_info failed: " <> err)
    Right info ->
      pure
        RoomInfo
          { name = fromMaybe mapping.platformKey.key (valueTextField "group_name" info)
          , description = ""
          , avatar = Nothing
          }

qqApiCall :: Context timeline -> Text -> Value -> IO (Either String Value)
qqApiCall context action parameters = do
  request0 <- parseRequest (Text.unpack (Text.dropWhileEnd (== '/') context.config.apiUrl <> "/" <> action))
  let authHeader =
        if Text.null context.config.accessToken
          then []
          else [("Authorization", Text.Encoding.encodeUtf8 ("Bearer " <> context.config.accessToken))]
      request =
        request0
          { method = "POST"
          , requestBody = RequestBodyLBS (encode parameters)
          , requestHeaders = ("Content-Type", "application/json") : authHeader <> request0.requestHeaders
          }
  response <- HTTP.loggedHttp context.logger qqHttpLogOptions request context.manager
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
firstNonempty = firstJust . filter (maybe False (not . Text.null))

firstJust :: [Maybe a] -> Maybe a
firstJust = listToMaybe . catMaybes

oneTextPart :: Text -> [MessagePart]
oneTextPart = pure . Text . plainInline

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

messageGroupId :: Text -> Maybe Text
messageGroupId key =
  case Text.splitOn "/" key of
    groupId : _ -> Just groupId
    [] -> Nothing
