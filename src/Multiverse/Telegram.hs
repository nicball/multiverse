module Multiverse.Telegram
  ( telegramBridge
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, forever, unless, when)
import Data.Aeson
import Data.ByteString qualified as ByteString
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.Function (on)
import Data.List (nubBy, sortOn)
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty, toList)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes, fromMaybe, isNothing, listToMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Config (InitialRoomMapping (..), TelegramBridgeConfig (..))
import Multiverse.Event hiding (eventId)
import Multiverse.HTTP (HttpLogOptions (..), defaultHttpLogOptions)
import Multiverse.HTTP qualified as HTTP
import Multiverse.Log
import Multiverse.Telegram.Rich qualified as Rich
import Multiverse.Timeline
import Multiverse.Types

data Context timeline = Context
  { timeline :: timeline
  , mappingStore :: MappingStore
  , logger :: Logger
  , manager :: Manager
  , config :: TelegramBridgeConfig
  , apiBase :: Text
  , fileBase :: Text
  }

telegramBridge :: Timeline timeline => BridgeRuntime timeline -> TelegramBridgeConfig -> IO (Bridge, TelegramBridgeConfig)
telegramBridge runtime initialConfig = do
  mappingStore <- openMappingStore initialConfig.mappingDb
  manager <- newManager tlsManagerSettings
  let provisionalContext =
        Context
          { timeline = runtime.timeline
          , mappingStore
          , logger = runtime.logger
          , manager
          , config = initialConfig
          , apiBase = "https://api.telegram.org/bot" <> initialConfig.botToken
          , fileBase = "https://api.telegram.org/file/bot" <> initialConfig.botToken
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
  logInfo runtime.logger ("telegram bridge started with mapping db " <> Text.pack config.mappingDb)
  pure
    ( Bridge
        { bridgeName = "telegram"
        , observe = observeTelegram context
        , reflect = reflectTelegram context
        }
    , config
    )

observeTelegram :: Timeline timeline => Context timeline -> IO ()
observeTelegram context =
  forever do
    offset <- fmap readTextInt <$> context.mappingStore.lookupState "telegram.update_offset"
    response <- telegramGetUpdates context offset
    case response of
      Left err -> do
        logError context.logger ("telegram observe error: " <> Text.pack err)
        threadDelay 5000000
      Right updates -> do
        mapM_ (observeUpdate context) updates
        case updates of
          [] -> pure ()
          _ -> context.mappingStore.setState "telegram.update_offset" (Text.pack (show (maximum (map (.updateId) updates) + 1)))

reflectTelegram :: Timeline timeline => Context timeline -> IO ()
reflectTelegram context =
  reflectForever context.timeline context.mappingStore "telegram.reflect_event" (reflectEvent context)

observeUpdate :: Timeline timeline => Context timeline -> TelegramUpdate -> IO ()
observeUpdate context update = do
  case update.editedMessage of
    Just message -> observeTelegramEdit context update.updateId message
    Nothing -> pure ()
  case update.message of
    Nothing -> pure ()
    Just message -> do
      room <- lookupConfiguredRoom context.mappingStore context.config.initialRooms (telegramRoomKey message.chat)
      case room of
        Nothing -> logDebug context.logger ("telegram ignoring unconfigured room " <> (telegramRoomKey message.chat).key)
        Just roomId -> do
          observeTelegramMembership context message roomId
          body <- telegramMessageBody context message
          case body of
            Nothing -> pure ()
            Just messageBody -> do
              userId <- ensureUser context message.from
              ensureRoomMembership context.logger context.timeline context.mappingStore "telegram" roomId userId
              replyTo <- telegramReplyTo context message
              let platformKey = telegramMessageKey message
                  event =
                    Event
                      { platformKey
                      , content =
                          SendMessage
                            SendMessageInfo
                              { sender = userId
                              , room = roomId
                              , replyTo
                              , forwardOf = Nothing
                              , body = messageBody
                              }
                      }
              submitted <- submitMessageMapped context.logger context.timeline context.mappingStore event
              case submitted of
                Right _ -> pure ()
                Left err -> fail ("telegram submit error: " <> show err)

observeTelegramMembership :: Timeline timeline => Context timeline -> TelegramMessage -> RoomId -> IO ()
observeTelegramMembership context message roomId = do
  forM_ message.newChatMembers \member -> submitMembership "join" member
  forM_ message.leftChatMember \member -> submitMembership "leave" member
 where
  submitMembership kind member = do
    user <- ensureUser context member
    previous <- getMembership context.timeline user roomId
    let platformKey =
          PlatformKey "telegram" kind ((telegramMessageKey message).key <> "/" <> Text.pack (show member.userId))
        content = if kind == "join" then JoinRoom user roomId previous else LeaveRoom user roomId previous
    result <- submitMapped context.logger context.timeline context.mappingStore Event {platformKey, content}
    case result of
      Left err -> logError context.logger ("telegram membership submit error: " <> Text.pack (show err))
      Right _ -> pure ()

observeTelegramEdit :: Timeline timeline => Context timeline -> Int -> TelegramMessage -> IO ()
observeTelegramEdit context updateId message = do
  room <- lookupConfiguredRoom context.mappingStore context.config.initialRooms (telegramRoomKey message.chat)
  case room of
    Nothing -> pure ()
    Just _ -> do
      target <- context.mappingStore.lookupTimelineId (telegramMessageKey message)
      case target of
        Nothing -> pure ()
        Just targetId -> do
          body <- telegramMessageBody context message
          case body of
            Nothing -> pure ()
            Just newBody -> do
              let key = PlatformKey "telegram" "edit" (Text.pack (show updateId))
              result <- submitMapped context.logger context.timeline context.mappingStore Event {platformKey = key, content = EditMessage (MessageId targetId) newBody}
              either (fail . show) (const (pure ())) result

telegramReplyTo :: Context timeline -> TelegramMessage -> IO (Maybe MessageId)
telegramReplyTo context message =
  case message.replyToMessage of
    Nothing -> pure Nothing
    Just repliedMessage -> do
      mapped <- context.mappingStore.lookupTimelineId (telegramMessageRefKey repliedMessage)
      pure (MessageId <$> mapped)

ensureUser :: Timeline timeline => Context timeline -> TelegramUser -> IO UserId
ensureUser context user = do
  mapped <- ensureUserMapped context.logger context.timeline context.mappingStore (telegramUserKey user) (pure (telegramUserName user))
  either (\err -> fail ("could not create telegram user: " <> show err)) pure mapped

reflectEvent :: Timeline timeline => Context timeline -> StoredEvent -> IO ()
reflectEvent context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "telegram" ->
          logDebug context.logger ("telegram not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
          let telegramRooms = filter (enabledRoom context.config.initialRooms) roomKeys
          mapM_ (sendToRoom info) telegramRooms
    EditMessage message body
      | stored.storedEvent.platformKey.platform /= "telegram" -> do
          original <- lookupSendMessage context.timeline message
          case original of
            Just info -> do
              unless (messageBlobs info.body == messageBlobs body) do
                logWarn context.logger "telegram cannot reflect changes to message attachments"
              editBody <- relayedBody context info {body = textBody body}
              alreadyMapped <- hasPlatformMapping context.mappingStore "telegram" stored.storedId
              unless alreadyMapped do
                targets <- telegramTextTargets context message
                forM_ (filter (enabledTelegramTarget context) targets) \(chatId, messageId) -> do
                  result <- telegramEditRichMessage context chatId messageId editBody
                  case result of
                    Left err | not ("message is not modified" `Text.isInfixOf` Text.toLower (Text.pack err)) ->
                      logError context.logger ("telegram edit reflect error: " <> Text.pack err)
                    Left _ ->
                      context.mappingStore.insertMapping (telegramEditKey stored.storedId chatId messageId) stored.storedId
                    Right _ -> do
                      let platformKey = telegramEditKey stored.storedId chatId messageId
                      context.mappingStore.insertMapping platformKey stored.storedId
                      logReflected context.logger stored platformKey
            _ -> pure ()
      | otherwise -> pure ()
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "telegram" ->
          logDebug context.logger ("telegram not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          alreadyMapped <- hasPlatformMapping context.mappingStore "telegram" stored.storedId
          unless alreadyMapped do
            targets <- telegramRetractTargets context message
            mapM_ deleteInChat (filter (enabledTelegramTarget context) targets)
    ChangeRoomNick user room nick
      | stored.storedEvent.platformKey.platform == "telegram" ->
          logDebug context.logger ("telegram not reflecting own-origin nick change " <> renderEventId stored.storedId)
      | otherwise -> do
          body <- nickBody context user nick
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
          forM_ (filter (enabledRoom context.config.initialRooms) roomKeys) \roomKey ->
            case readMaybe (Text.unpack roomKey.key) of
              Nothing -> logWarn context.logger ("telegram ignoring invalid chat id " <> roomKey.key)
              Just chatId -> do
                alreadySent <- telegramMessageMappedToChat context stored.storedId chatId
                unless alreadySent do
                  result <- telegramSendRichMessage context chatId Nothing body
                  case result of
                    Left err -> logError context.logger ("telegram nick reflect error: " <> Text.pack err)
                    Right sentMessage -> do
                      let platformKey = telegramMessageKey sentMessage
                      context.mappingStore.insertMapping platformKey stored.storedId
                      logReflected context.logger stored platformKey
    _ -> pure ()
 where
  -- A whole message body is reflected as one rich message: the room's mapping
  -- row is what stops the event from being reflected into the same chat twice.
  sendToRoom info roomKey =
    case readMaybe (Text.unpack roomKey.key) of
      Nothing -> pure ()
      Just chatId -> do
        alreadySent <- telegramMessageMappedToChat context stored.storedId chatId
        unless alreadySent do
          body <- relayedBody context info
          replyTo <- telegramReplyMessageId context roomKey info.replyTo
          result <- telegramSendRichMessage context chatId replyTo body
          case result of
            Left err -> logError context.logger ("telegram reflect error: " <> Text.pack err)
            Right message -> do
              let platformKey = telegramMessageKey message
              context.mappingStore.insertMapping platformKey stored.storedId
              logReflected context.logger stored platformKey
  deleteInChat (chatId, messageId) = do
    result <- telegramDeleteMessage context chatId messageId
    case result of
      Left err -> logError context.logger ("telegram retract reflect error: " <> Text.pack err)
      Right () -> do
        let platformKey = telegramRetractionKey stored.storedId
        context.mappingStore.insertMapping platformKey stored.storedId
        logReflected context.logger stored platformKey

-- | The relayed message is attributed to the relaying account, so the sender is
-- part of the body instead.
relayedBody :: Timeline timeline => Context timeline -> SendMessageInfo -> IO Message
relayedBody context info = do
  user <- getUserInfo context.timeline info.sender (Just info.room)
  let sender = maybe "unknown" (.name) user
  pure (Text (Bold (plainInline sender) :| [Plain ": "]) :| toList info.body)

nickBody :: Timeline timeline => Context timeline -> UserId -> Text -> IO Message
nickBody context user nick = do
  userInfo <- getUserInfo context.timeline user Nothing
  let previous = maybe "unknown" (.name) userInfo
  pure (Text (Italic (plainInline ("* " <> previous <> " is now known as " <> nick)) :| []) :| [])

-- | Telegram reports no identifier for an edit, so the applied edit is recorded
-- under a key derived from the platform message it changed.
telegramEditKey :: EventId -> Integer -> Int -> PlatformKey
telegramEditKey eventId_ chatId messageId =
  PlatformKey
    "telegram"
    "edit"
    (renderEventId eventId_ <> "/" <> Text.pack (show chatId) <> "/" <> Text.pack (show messageId))

telegramMessageMappedToChat :: Context timeline -> EventId -> Integer -> IO Bool
telegramMessageMappedToChat context eventId_ chatId = do
  targets <- telegramTargets context eventId_
  pure (any ((== chatId) . fst) targets)

-- | Telegram cannot say which of the messages sent for an event carries its
-- text, but the text part is always sent first and message ids grow within a
-- chat, so the smallest id in each chat is the text part.
telegramTextTargets :: Context timeline -> MessageId -> IO [(Integer, Int)]
telegramTextTargets context message = do
  targets <- telegramRetractTargets context message
  pure (nubBy ((==) `on` fst) (sortOn id targets))

enabledTelegramTarget :: Context timeline -> (Integer, Int) -> Bool
enabledTelegramTarget context (chatId, _) =
  enabledRoom context.config.initialRooms (PlatformKey "telegram" "room" (Text.pack (show chatId)))

telegramRetractTargets :: Context timeline -> MessageId -> IO [(Integer, Int)]
telegramRetractTargets context message = telegramTargets context (messageEventId message)

telegramTargets :: Context timeline -> EventId -> IO [(Integer, Int)]
telegramTargets context eventId_ = do
  keys <- lookupMappedKeys context.mappingStore "telegram" "message" eventId_
  pure (mapMaybe telegramMessageTarget keys)

telegramMessageTarget :: Text -> Maybe (Integer, Int)
telegramMessageTarget key =
  case Text.splitOn "/" key of
    [chatIdText, messageIdText] -> (,) <$> readMaybe (Text.unpack chatIdText) <*> readMaybe (Text.unpack messageIdText)
    _ -> Nothing

telegramReplyMessageId :: Context timeline -> PlatformKey -> Maybe MessageId -> IO (Maybe Int)
telegramReplyMessageId _ _ Nothing = pure Nothing
telegramReplyMessageId context roomKey (Just message) = do
  keys <- lookupMappedKeys context.mappingStore "telegram" "message" (messageEventId message)
  pure (listToMaybe (mapMaybe (matchingTelegramMessage roomKey.key) keys))

matchingTelegramMessage :: Text -> Text -> Maybe Int
matchingTelegramMessage chatId key =
  case Text.splitOn "/" key of
    [messageChatId, messageIdText]
      | messageChatId == chatId -> readMaybe (Text.unpack messageIdText)
    _ -> Nothing

telegramGetUpdates :: Context timeline -> Maybe Int -> IO (Either String [TelegramUpdate])
telegramGetUpdates context offset = do
  let query =
        [ ("timeout", Just (ByteString.Char8.pack (show context.config.pollTimeoutSeconds)))
        , ("allowed_updates", Just "[\"message\",\"edited_message\"]")
        ]
          <> maybe [] (\value -> [("offset", Just (ByteString.Char8.pack (show value)))]) offset
  telegramCall context "getUpdates" (setQueryString query)

-- | Telegram responses all share the API base and the same result envelope, so
-- every endpoint is one call to this helper.
telegramCall :: FromJSON a => Context timeline -> Text -> (Request -> Request) -> IO (Either String a)
telegramCall context method modify = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/" <> method))
  response <- loggedHttp context.logger (modify request0) context.manager
  pure (decodeResponse response)

-- | One media part of a rich message. A blob the bridge already uploaded is
-- referenced by its file id; anything else travels with the request.
data RichAttachment = RichAttachment
  { attachmentBlob :: BlobId
  , attachmentMethod :: Text
  , attachmentField :: Text
  , attachmentBytes :: ByteString.ByteString
  , attachmentFileId :: Maybe Text
  }

telegramSendRichMessage :: Timeline timeline => Context timeline -> Integer -> Maybe Int -> Message -> IO (Either String TelegramMessage)
telegramSendRichMessage context chatId replyTo body =
  telegramRichCall
    context
    "sendRichMessage"
    ( [("chat_id", ByteString.Char8.pack (show chatId))]
        <> maybe [] (\messageId -> [("reply_to_message_id", ByteString.Char8.pack (show messageId))]) replyTo
    )
    body

telegramEditRichMessage :: Timeline timeline => Context timeline -> Integer -> Int -> Message -> IO (Either String TelegramMessage)
telegramEditRichMessage context chatId messageId body =
  telegramRichCall
    context
    "editMessageText"
    [ ("chat_id", ByteString.Char8.pack (show chatId))
    , ("message_id", ByteString.Char8.pack (show messageId))
    ]
    body

-- | Send a whole message body as one rich message. Media stays where the
-- timeline put it because the blocks reference it and the upload happens in the
-- same multipart request, so a reflected message keeps its part order.
telegramRichCall :: Timeline timeline => Context timeline -> Text -> [(ByteString.ByteString, ByteString.ByteString)] -> Message -> IO (Either String TelegramMessage)
telegramRichCall context methodName fields body = do
  attachments <- traverse attachment (messageBlobs body)
  let indexed = zip [(0 :: Int) ..] attachments
      fileParts = [(attachName index, part) | (index, part) <- indexed, isNothing part.attachmentFileId]
      blocks = Rich.messageBlocks (blockFor indexed) body
      boundary = foldl chooseBoundary "multiverse-telegram-rich" (map (\(_, part) -> part.attachmentBytes) fileParts)
      renderField name value =
        "--" <> boundary <> "\r\nContent-Disposition: form-data; name=\"" <> name <> "\"\r\n\r\n" <> value <> "\r\n"
      renderFile (name, part) =
        ByteString.concat
          [ "--"
          , boundary
          , "\r\nContent-Disposition: form-data; name=\""
          , Text.Encoding.encodeUtf8 name
          , "\"; filename=\""
          , Text.Encoding.encodeUtf8 name
          , "\"\r\nContent-Type: "
          , Text.Encoding.encodeUtf8 (sniffMimeType part.attachmentBytes)
          , "\r\n\r\n"
          , part.attachmentBytes
          , "\r\n"
          ]
      formBody =
        ByteString.concat
          ( map (uncurry renderField) fields
              <> [renderField "rich_message" (LByteString.toStrict (encode (object ["blocks" .= blocks])))]
              <> map renderFile fileParts
              <> ["--", boundary, "--\r\n"]
          )
  result <- telegramCall context methodName \request ->
    request
      { method = "POST"
      , requestBody = RequestBodyBS formBody
      , requestHeaders = [("Content-Type", "multipart/form-data; boundary=" <> boundary)]
      }
  case result of
    Left err -> pure (Left err)
    Right sent -> do
      recordUploadedFiles context attachments sent
      pure (Right sent)
 where
  attachment :: (BlobType, BlobId) -> IO RichAttachment
  attachment (kind, blobId) = do
    stored <- getEvent context.timeline (blobEventId blobId)
    bytes <- case fmap (.storedEvent.content) stored of
      Just (CreateBlob content) -> pure content
      _ -> fail "telegram rich message blob is missing"
    let (mediaMethod, mediaField) = telegramMediaMethod kind bytes
    fileId <- telegramMappedFileId context mediaMethod blobId
    pure
      RichAttachment
        { attachmentBlob = blobId
        , attachmentMethod = mediaMethod
        , attachmentField = mediaField
        , attachmentBytes = bytes
        , attachmentFileId = fileId
        }
  -- A block only names its media, so the lookup keeps the upload reference or
  -- file id next to the blob the timeline named.
  blockFor indexed _ blob =
    case [mediaBlockValue index part | (index, part) <- indexed, blobEventId part.attachmentBlob == blobEventId blob] of
      blockValue : _ -> blockValue
      [] -> object ["type" .= ("document" :: Text), "media" .= ("" :: Text)]
   where
    mediaBlockValue index part =
      object
        [ "type" .= part.attachmentField
        , "media" .= maybe (attachName index) id part.attachmentFileId
        ]

attachName :: Int -> Text
attachName index = "file_" <> Text.pack (show index)

-- | The bot API answers with the media it ingested, in block order, so the file
-- ids of freshly uploaded blobs can be remembered for later messages.
recordUploadedFiles :: Context timeline -> [RichAttachment] -> TelegramMessage -> IO ()
recordUploadedFiles context attachments sent =
  case sent.richMessage >>= either (const Nothing) Just . Rich.parseRichMessage of
    Nothing -> pure ()
    Just rich -> do
      let refs = Rich.richMessageMedia rich
      when (length refs == length attachments) do
        forM_ (zip attachments refs) \(part, ref) ->
          when (isNothing part.attachmentFileId) do
            let blobEvent = blobEventId part.attachmentBlob
            forM_ ref.fileUniqueId \uniqueId -> context.mappingStore.insertMapping (telegramSourceKey uniqueId) blobEvent
            forM_ ref.fileId \fileId -> context.mappingStore.insertMapping (telegramBlobKey part.attachmentMethod fileId) blobEvent

-- | Media this bridge may send by file id instead of uploading it again. The key
-- carries the upload method because a file id is only valid for the method that
-- minted it.
telegramBlobKey :: Text -> Text -> PlatformKey
telegramBlobKey methodName fileId = PlatformKey "telegram" "blob" (methodName <> ":" <> fileId)

telegramSourceKey :: Text -> PlatformKey
telegramSourceKey uniqueId = PlatformKey "telegram" "blob" uniqueId

telegramMappedFileId :: Context timeline -> Text -> BlobId -> IO (Maybe Text)
telegramMappedFileId context methodName blobId = do
  keys <- lookupMappedKeys context.mappingStore "telegram" "blob" (blobEventId blobId)
  pure (listToMaybe [fileId | Just fileId <- map (Text.stripPrefix (methodName <> ":")) keys])

-- | Animated images have to be sent as animations; a photo would flatten them
-- to a still frame. The method name doubles as the mapping key for file ids.
telegramMediaMethod :: BlobType -> ByteString.ByteString -> (Text, Text)
telegramMediaMethod kind bytes = case kind of
  ImageBlob
    | isAnimatedImage bytes -> ("sendAnimation", "animation")
    | otherwise -> ("sendPhoto", "photo")
  VideoBlob -> ("sendVideo", "video")
  AudioBlob -> ("sendAudio", "audio")
  FileBlob -> ("sendDocument", "document")

chooseBoundary :: ByteString.ByteString -> ByteString.ByteString -> ByteString.ByteString
chooseBoundary boundary bytes
  | boundary `ByteString.isInfixOf` bytes = chooseBoundary (boundary <> "x") bytes
  | otherwise = boundary

telegramDeleteMessage :: Context timeline -> Integer -> Int -> IO (Either String ())
telegramDeleteMessage context chatId messageId =
  telegramCall context "deleteMessage"
    ( urlEncodedBody
        [ ("chat_id", ByteString.Char8.pack (show chatId))
        , ("message_id", ByteString.Char8.pack (show messageId))
        ]
    )

fetchInitialRoomInfo :: Context timeline -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo context mapping =
  case readMaybe (Text.unpack mapping.platformKey.key) of
    Nothing -> fail ("telegram room key is not a chat id: " <> Text.unpack mapping.platformKey.key)
    Just chatId -> do
      result <- telegramGetChat context chatId
      case result of
        Left err -> fail ("telegram getChat failed: " <> err)
        Right chat -> pure RoomInfo {name = telegramChatName chat, description = "", avatar = Nothing}

telegramGetChat :: Context timeline -> Integer -> IO (Either String TelegramChat)
telegramGetChat context chatId =
  telegramCall context "getChat"
    (setQueryString [("chat_id", Just (ByteString.Char8.pack (show chatId)))])

loggedHttp :: Logger -> Request -> Manager -> IO (Response LByteString.ByteString)
loggedHttp logger = HTTP.loggedHttp logger telegramHttpLogOptions

telegramHttpLogOptions :: HttpLogOptions
telegramHttpLogOptions =
  defaultHttpLogOptions {redactUrl = redactTelegramUrl}

redactTelegramUrl :: Text -> Text
redactTelegramUrl url =
  case Text.breakOn "/bot" url of
    (prefix, rest)
      | Text.null rest -> url
      | otherwise ->
          let suffix = Text.dropWhile (/= '/') (Text.drop 4 rest)
           in prefix <> "/bot<redacted>" <> suffix

decodeResponse :: FromJSON a => Response LByteString.ByteString -> Either String a
decodeResponse response =
  case decodeTelegramResponse response.responseBody of
    Left err -> Left err
    Right telegramResponse ->
      if telegramResponse.ok
        then maybe (Left "missing result") Right telegramResponse.result
        else Left (Text.unpack (maybe "telegram api error" id telegramResponse.description))

decodeTelegramResponse :: FromJSON a => LByteString.ByteString -> Either String (TelegramResponse a)
decodeTelegramResponse = eitherDecode

telegramUserKey :: TelegramUser -> PlatformKey
telegramUserKey user = PlatformKey "telegram" "user" (Text.pack (show user.userId))

telegramRoomKey :: TelegramChat -> PlatformKey
telegramRoomKey chat = PlatformKey "telegram" "room" (Text.pack (show chat.chatId))

telegramMessageKey :: TelegramMessage -> PlatformKey
telegramMessageKey message = telegramChatMessageKey message.chat.chatId message.messageId

telegramMessageRefKey :: TelegramMessageRef -> PlatformKey
telegramMessageRefKey message = telegramChatMessageKey message.chat.chatId message.messageId

telegramChatMessageKey :: Integer -> Int -> PlatformKey
telegramChatMessageKey chatId messageId =
  PlatformKey "telegram" "message" (Text.pack (show chatId) <> "/" <> Text.pack (show messageId))

telegramRetractionKey :: EventId -> PlatformKey
telegramRetractionKey eventId_ = PlatformKey "telegram" "retraction" (renderEventId eventId_)

telegramUserName :: TelegramUser -> Text
telegramUserName user =
  Text.intercalate " " (filter (not . Text.null) (catMaybes [user.firstName, user.lastName]))

telegramChatName :: TelegramChat -> Text
telegramChatName chat = maybe (Text.pack (show chat.chatId)) id chat.title

telegramMessageBody :: Timeline timeline => Context timeline -> TelegramMessage -> IO (Maybe Message)
telegramMessageBody context message =
  case message.richMessage of
    Just value ->
      case Rich.parseRichMessage value of
        Right rich -> Rich.renderRichMessage (importMediaSource context) rich
        Left err -> do
          logDebug context.logger ("telegram rich message could not be parsed: " <> err)
          pure Nothing
    Nothing -> do
      media <- traverse importMessageMedia message.media
      let textParts = maybe [] (NonEmpty.toList . (`telegramMessageFromEntities` message.entities)) message.text
      pure (nonEmpty (textParts <> maybe [] (\(kind, blob) -> [Blob kind blob]) media))
 where
  importMessageMedia (kind, telegramMedia) = do
    blob <- importMediaSource context kind (telegramMediaRef telegramMedia)
    pure (kind, blob)

-- | A blob is content addressed, so a platform resource the bridge already knows
-- is reused and only an unknown one is downloaded and submitted.
importMediaSource :: Timeline timeline => Context timeline -> BlobType -> Rich.MediaRef -> IO BlobId
importMediaSource context kind ref = do
  fileId <- maybe (fail "telegram media lacks file_id") pure ref.fileId
  let sourceKey = telegramSourceKey (fromMaybe fileId ref.fileUniqueId)
  existing <- context.mappingStore.lookupTimelineId sourceKey
  content <- case existing of
    Just blobEvent -> do
      stored <- getEvent context.timeline blobEvent
      case fmap (.storedEvent.content) stored of
        Just (CreateBlob bytes) -> pure bytes
        _ -> telegramDownloadFile context fileId
    Nothing -> telegramDownloadFile context fileId
  blob <- case existing of
    Just eventId -> pure (BlobId eventId)
    Nothing ->
      either (fail . show) pure
        =<< submitBlob context.logger context.timeline context.mappingStore sourceKey content
  let (methodName, _) = telegramMediaMethod kind content
  context.mappingStore.insertMapping (telegramBlobKey methodName fileId) (blobEventId blob)
  pure blob

telegramMediaRef :: TelegramMedia -> Rich.MediaRef
telegramMediaRef media =
  Rich.MediaRef {fileId = Just media.fileId, fileUniqueId = media.fileUniqueId}

telegramDownloadFile :: Context timeline -> Text -> IO ByteString.ByteString
telegramDownloadFile context fileId = do
  file <-
    either fail pure
      =<< (telegramCall context "getFile" (setQueryString [("file_id", Just (Text.Encoding.encodeUtf8 fileId))]) :: IO (Either String TelegramFile))
  path <- maybe (fail "telegram getFile response lacks file_path") pure file.filePath
  request <- parseRequest (Text.unpack (context.fileBase <> "/" <> path))
  download <- loggedHttp context.logger request context.manager
  pure (LByteString.toStrict download.responseBody)

data TelegramFile = TelegramFile {filePath :: Maybe Text}

instance FromJSON TelegramFile where
  parseJSON = withObject "TelegramFile" \obj -> TelegramFile <$> obj .:? "file_path"

telegramMessageFromEntities :: Text -> [TelegramEntity] -> Message
telegramMessageFromEntities text entities =
  case wholeBlockQuote text entities of
    Just quote -> BlockQuote (Text quote :| []) :| []
    Nothing -> Text (telegramInline text entities) :| []

wholeBlockQuote :: Text -> [TelegramEntity] -> Maybe InlineText
wholeBlockQuote text entities =
  case [entity | entity <- entities, entity.entityType == "blockquote", entity.offset == 0, entity.length >= Text.length text] of
    _ : _ -> Just (telegramInline text (filter ((/= "blockquote") . (.entityType)) entities))
    [] -> Nothing

telegramInline :: Text -> [TelegramEntity] -> InlineText
telegramInline text entities =
  case nonEmpty (go 0 (filter supportedInlineEntity (sortTelegramEntities entities))) of
    Just inline -> inline
    Nothing -> Plain text :| []
 where
  go index [] =
    [Plain (Text.drop index text) | index < Text.length text]
  go index (entity : rest) =
    let before = Text.take (entity.offset - index) (Text.drop index text)
        body = Text.take entity.length (Text.drop entity.offset text)
        afterIndex = entity.offset + entity.length
        beforePart = [Plain before | not (Text.null before)]
     in beforePart <> entityPart entity body <> go afterIndex rest

supportedInlineEntity :: TelegramEntity -> Bool
supportedInlineEntity entity = entity.entityType `elem` ["bold", "italic", "code", "text_link", "url"]

sortTelegramEntities :: [TelegramEntity] -> [TelegramEntity]
sortTelegramEntities = sortOn (.offset)

entityPart :: TelegramEntity -> Text -> [InlineTextPart]
entityPart entity body =
  let inline = Plain body :| []
   in case entity.entityType of
        "bold" -> [Bold inline]
        "italic" -> [Italic inline]
        "code" -> [InlineQuote inline]
        "text_link" -> [Link inline (maybe body id entity.url)]
        "url" -> [Link inline body]
        _ -> [Plain body]

readTextInt :: Text -> Int
readTextInt = maybe 0 id . readMaybe . Text.unpack

data TelegramResponse a = TelegramResponse
  { ok :: Bool
  , result :: Maybe a
  , description :: Maybe Text
  }

instance FromJSON a => FromJSON (TelegramResponse a) where
  parseJSON = withObject "TelegramResponse" \obj ->
    TelegramResponse
      <$> obj .: "ok"
      <*> obj .:? "result"
      <*> obj .:? "description"

data TelegramUpdate = TelegramUpdate
  { updateId :: Int
  , message :: Maybe TelegramMessage
  , editedMessage :: Maybe TelegramMessage
  }

instance FromJSON TelegramUpdate where
  parseJSON = withObject "TelegramUpdate" \obj ->
    TelegramUpdate
      <$> obj .: "update_id"
      <*> obj .:? "message"
      <*> obj .:? "edited_message"

data TelegramMessage = TelegramMessage
  { messageId :: Int
  , from :: TelegramUser
  , chat :: TelegramChat
  , text :: Maybe Text
  , entities :: [TelegramEntity]
  , replyToMessage :: Maybe TelegramMessageRef
  , media :: Maybe (BlobType, TelegramMedia)
  , richMessage :: Maybe Value
  , newChatMembers :: [TelegramUser]
  , leftChatMember :: Maybe TelegramUser
  }

instance FromJSON TelegramMessage where
  parseJSON = withObject "TelegramMessage" \obj -> do
    photos <- obj .:? "photo" .!= []
    photo <- pure (case reverse photos of value : _ -> Just value; [] -> Nothing)
    video <- obj .:? "video"
    videoNote <- obj .:? "video_note"
    animation <- obj .:? "animation"
    audio <- obj .:? "audio"
    voice <- obj .:? "voice"
    document <- obj .:? "document"
    sticker <- obj .:? "sticker"
    let animationIsImage = case animation of
          Just media -> "image/" `Text.isPrefixOf` fromMaybe "" media.mimeType
          _ -> False
        imageAnimation = if animationIsImage then animation else Nothing
        videoAnimation = if animationIsImage then Nothing else animation
    TelegramMessage
      <$> obj .: "message_id"
      <*> obj .: "from"
      <*> obj .: "chat"
      <*> ((<|>) <$> obj .:? "text" <*> obj .:? "caption")
      <*> ((maybe [] id) <$> ((<|>) <$> obj .:? "entities" <*> obj .:? "caption_entities"))
      <*> obj .:? "reply_to_message"
      <*> pure ((ImageBlob,) <$> (photo <|> sticker <|> imageAnimation) <|> (VideoBlob,) <$> (video <|> videoNote <|> videoAnimation) <|> (AudioBlob,) <$> (audio <|> voice) <|> (FileBlob,) <$> document)
      <*> obj .:? "rich_message"
      <*> obj .:? "new_chat_members" .!= []
      <*> obj .:? "left_chat_member"

data TelegramMedia = TelegramMedia
  { fileId :: Text
  , fileUniqueId :: Maybe Text
  , mimeType :: Maybe Text
  }

instance FromJSON TelegramMedia where
  parseJSON = withObject "TelegramMedia" \obj ->
    TelegramMedia
      <$> obj .: "file_id"
      <*> obj .:? "file_unique_id"
      <*> obj .:? "mime_type"

data TelegramEntity = TelegramEntity
  { entityType :: Text
  , offset :: Int
  , length :: Int
  , url :: Maybe Text
  }

instance FromJSON TelegramEntity where
  parseJSON = withObject "TelegramEntity" \obj ->
    TelegramEntity
      <$> obj .: "type"
      <*> obj .: "offset"
      <*> obj .: "length"
      <*> obj .:? "url"

data TelegramMessageRef = TelegramMessageRef
  { messageId :: Int
  , chat :: TelegramChat
  }

instance FromJSON TelegramMessageRef where
  parseJSON = withObject "TelegramMessageRef" \obj ->
    TelegramMessageRef
      <$> obj .: "message_id"
      <*> obj .: "chat"

data TelegramUser = TelegramUser
  { userId :: Integer
  , firstName :: Maybe Text
  , lastName :: Maybe Text
  }

instance FromJSON TelegramUser where
  parseJSON = withObject "TelegramUser" \obj ->
    TelegramUser
      <$> obj .: "id"
      <*> obj .:? "first_name"
      <*> obj .:? "last_name"

data TelegramChat = TelegramChat
  { chatId :: Integer
  , title :: Maybe Text
  }

instance FromJSON TelegramChat where
  parseJSON = withObject "TelegramChat" \obj ->
    TelegramChat
      <$> obj .: "id"
      <*> obj .:? "title"
