module Multiverse.Telegram
  ( telegramBridge
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, forever, unless)
import Data.Aeson
import Data.ByteString qualified as ByteString
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.Function (on)
import Data.List (nubBy, sortOn)
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Config (InitialRoomMapping (..), TelegramBridgeConfig (..))
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
  forever do
    afterEventId <- fmap (fmap EventId) (context.mappingStore.lookupState "telegram.reflect_event")
    storedEvents <- getEventsAfter context.timeline afterEventId
    mapM_ (reflectEvent context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "telegram.reflect_event" (renderEventId ((last storedEvents).storedId))

observeUpdate :: Timeline timeline => Context timeline -> TelegramUpdate -> IO ()
observeUpdate context update = do
  case update.editedMessage of
    Just message -> observeTelegramEdit context update.updateId message
    Nothing -> pure ()
  case update.message of
    Nothing -> pure ()
    Just message -> do
      room <- configuredRoom context message.chat
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
              submitted <- submitMessageMapped context.timeline context.mappingStore event
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
    let platformKey =
          PlatformKey "telegram" kind ((telegramMessageKey message).key <> "/" <> Text.pack (show member.userId))
        content = if kind == "join" then JoinRoom user roomId else LeaveRoom user roomId
    result <- submitMapped context.timeline context.mappingStore Event {platformKey, content}
    case result of
      Left err -> logError context.logger ("telegram membership submit error: " <> Text.pack (show err))
      Right _ -> pure ()

observeTelegramEdit :: Timeline timeline => Context timeline -> Int -> TelegramMessage -> IO ()
observeTelegramEdit context updateId message = do
  room <- configuredRoom context message.chat
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
              result <- submitMapped context.timeline context.mappingStore Event {platformKey = key, content = EditMessage (MessageId targetId) newBody}
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
  let platformKey = telegramUserKey user
  existing <- context.mappingStore.lookupTimelineId platformKey
  case existing of
    Just eventId -> pure (UserId eventId)
    Nothing -> do
      let event = Event {platformKey, content = CreateUser UserInfo {name = telegramUserName user, avatar = Nothing}}
      mapped <- submitMapped context.timeline context.mappingStore event
      case mapped of
        Right eventId -> pure (UserId eventId)
        Left err -> fail ("could not create telegram user: " <> show err)

configuredRoom :: Context timeline -> TelegramChat -> IO (Maybe RoomId)
configuredRoom context chat = do
  let platformKey = telegramRoomKey chat
  if enabledRoom context.config.initialRooms platformKey
    then fmap RoomId <$> context.mappingStore.lookupTimelineId platformKey
    else pure Nothing

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
          original <- getEvent context.timeline (messageEventId message)
          case fmap (.storedEvent.content) original of
            Just (SendMessage info) -> do
              unless (messageBlobs info.body == messageBlobs body) do
                logWarn context.logger "telegram cannot reflect changes to message attachments"
              text <- renderRelayedMessage context.timeline info {body = textBody body}
              alreadyMapped <- hasPlatformMapping context.mappingStore "telegram" stored.storedId
              unless alreadyMapped do
                targets <- telegramTextTargets context message
                forM_ (filter (enabledTelegramTarget context) targets) \(chatId, messageId) -> do
                  result <- telegramEditText context chatId messageId text
                  case result of
                    Left err | not ("message is not modified" `Text.isInfixOf` Text.toLower (Text.pack err)) ->
                      logError context.logger ("telegram edit reflect error: " <> Text.pack err)
                    _ -> context.mappingStore.insertMapping (telegramEditKey stored.storedId chatId messageId) stored.storedId
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
          text <- renderNickChange context.timeline user nick
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
          forM_ (filter (enabledRoom context.config.initialRooms) roomKeys) \roomKey ->
            case readMaybe (Text.unpack roomKey.key) of
              Nothing -> logWarn context.logger ("telegram ignoring invalid chat id " <> roomKey.key)
              Just chatId -> do
                alreadySent <- telegramMessageMappedToChat context stored.storedId roomKey.key
                unless alreadySent do
                  result <- telegramSendMessage context chatId Nothing text
                  case result of
                    Left err -> logError context.logger ("telegram nick reflect error: " <> Text.pack err)
                    Right sentMessage ->
                      context.mappingStore.insertMapping (telegramMessageKey sentMessage) stored.storedId
    _ -> pure ()
 where
  -- A message is reflected part by part; each part's mapping row is written as
  -- soon as that part is delivered, and the room's mapping row is what stops the
  -- event from being reflected into the same chat twice. An interrupted message
  -- is therefore not resumed where it stopped.
  sendToRoom info roomKey =
    case readMaybe (Text.unpack roomKey.key) of
      Nothing -> pure ()
      Just chatId -> do
        alreadySent <- telegramMessageMappedToChat context stored.storedId roomKey.key
        unless alreadySent do
          text <- renderRelayedMessage context.timeline info {body = textBody info.body}
          replyTo <- telegramReplyMessageId context roomKey info.replyTo
          let sendPart action = do
                result <- action
                case result of
                  Left err -> logError context.logger ("telegram reflect error: " <> Text.pack err)
                  Right message ->
                    context.mappingStore.insertMapping (telegramMessageKey message) stored.storedId
          sendPart (telegramSendMessage context chatId replyTo text)
          forM_ (messageBlobs info.body) \(kind, blobId) -> do
            blob <- getEvent context.timeline (blobEventId blobId)
            case fmap (.storedEvent.content) blob of
              Just (CreateBlob bytes) -> sendPart (sendMappedMedia context chatId replyTo kind blobId bytes)
              _ -> logError context.logger "telegram media blob is missing"
  deleteInChat (chatId, messageId) = do
    result <- telegramDeleteMessage context chatId messageId
    case result of
      Left err -> logError context.logger ("telegram retract reflect error: " <> Text.pack err)
      Right () -> context.mappingStore.insertMapping (telegramRetractionKey stored.storedId) stored.storedId

-- | Telegram reports no identifier for an edit, so the applied edit is recorded
-- under a key derived from the platform message it changed.
telegramEditKey :: EventId -> Integer -> Int -> PlatformKey
telegramEditKey eventId_ chatId messageId =
  PlatformKey
    "telegram"
    "edit"
    (renderEventId eventId_ <> "/" <> Text.pack (show chatId) <> "/" <> Text.pack (show messageId))

telegramMessageMappedToChat :: Context timeline -> EventId -> Text -> IO Bool
telegramMessageMappedToChat context eventId_ chatId = do
  keys <- context.mappingStore.lookupPlatformKeys eventId_
  pure (any (isJust . matchingTelegramMessage chatId) keys)

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
telegramRetractTargets context message = do
  keys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
  pure (mapMaybe telegramMessageTarget keys)

telegramMessageTarget :: PlatformKey -> Maybe (Integer, Int)
telegramMessageTarget platformKey
  | platformKey.platform /= "telegram" = Nothing
  | platformKey.entityType /= "message" = Nothing
  | otherwise =
      case Text.splitOn "/" platformKey.key of
        [chatIdText, messageIdText] -> (,) <$> readMaybe (Text.unpack chatIdText) <*> readMaybe (Text.unpack messageIdText)
        _ -> Nothing

telegramReplyMessageId :: Context timeline -> PlatformKey -> Maybe MessageId -> IO (Maybe Int)
telegramReplyMessageId _ _ Nothing = pure Nothing
telegramReplyMessageId context roomKey (Just message) = do
  keys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
  pure case mapMaybe (matchingTelegramMessage roomKey.key) keys of
    messageId : _ -> Just messageId
    [] -> Nothing

matchingTelegramMessage :: Text -> PlatformKey -> Maybe Int
matchingTelegramMessage chatId platformKey
  | platformKey.platform /= "telegram" = Nothing
  | platformKey.entityType /= "message" = Nothing
  | otherwise =
      case Text.splitOn "/" platformKey.key of
        [messageChatId, messageIdText]
          | messageChatId == chatId -> readMaybe (Text.unpack messageIdText)
        _ -> Nothing

telegramGetUpdates :: Context timeline -> Maybe Int -> IO (Either String [TelegramUpdate])
telegramGetUpdates context offset = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/getUpdates"))
  let query =
        [ ("timeout", Just (ByteString.Char8.pack (show context.config.pollTimeoutSeconds)))
        , ("allowed_updates", Just "[\"message\",\"edited_message\"]")
        ]
          <> maybe [] (\value -> [("offset", Just (ByteString.Char8.pack (show value)))]) offset
      request = setQueryString query request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

telegramSendMessage :: Context timeline -> Integer -> Maybe Int -> RenderedMessage -> IO (Either String TelegramMessage)
telegramSendMessage context chatId replyTo text = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/sendMessage"))
  let request =
        urlEncodedBody
          ( [ ("chat_id", ByteString.Char8.pack (show chatId))
            , ("text", Text.encodeUtf8 (telegramHtml text))
            , ("parse_mode", "HTML")
            ]
              <> maybe [] (\messageId -> [("reply_to_message_id", ByteString.Char8.pack (show messageId))]) replyTo
          )
          request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

telegramEditText :: Context timeline -> Integer -> Int -> RenderedMessage -> IO (Either String TelegramMessage)
telegramEditText context chatId messageId text = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/editMessageText"))
  let request = urlEncodedBody
        [ ("chat_id", ByteString.Char8.pack (show chatId))
        , ("message_id", ByteString.Char8.pack (show messageId))
        , ("text", Text.encodeUtf8 (telegramHtml text))
        , ("parse_mode", "HTML")
        ] request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

sendMappedMedia :: Context timeline -> Integer -> Maybe Int -> BlobType -> BlobId -> ByteString.ByteString -> IO (Either String TelegramMessage)
sendMappedMedia context chatId replyTo kind blobId bytes = do
  let method@(methodName, _) = telegramMediaMethod kind bytes
      upload = telegramSendMedia context chatId replyTo method bytes
  existing <- telegramMappedFileId context methodName blobId
  result <- case existing of
    Nothing -> upload
    Just fileId -> do
      sent <- telegramSendMediaId context chatId replyTo method fileId
      case sent of
        Right message -> pure (Right message)
        Left err -> do
          logWarn context.logger ("telegram file ID rejected; uploading again: " <> Text.pack err)
          upload
  case result of
    Right message -> case message.media of
      Just (returnedKind, media) | returnedKind == kind -> do
        let blobEvent = blobEventId blobId
        context.mappingStore.insertMapping (telegramSourceKey media) blobEvent
        context.mappingStore.insertMapping (telegramBlobKey methodName media.fileId) blobEvent
      _ -> pure ()
    Left _ -> pure ()
  pure result

-- | Media this bridge may send by file id instead of uploading it again. The key
-- carries the upload method because a file id is only valid for the method that
-- minted it.
telegramBlobKey :: Text -> Text -> PlatformKey
telegramBlobKey methodName fileId = PlatformKey "telegram" "blob" (methodName <> ":" <> fileId)

telegramMappedFileId :: Context timeline -> Text -> BlobId -> IO (Maybe Text)
telegramMappedFileId context methodName blobId = do
  keys <- context.mappingStore.lookupPlatformKeys (blobEventId blobId)
  pure case
    [ fileId
    | key <- keys
    , key.platform == "telegram"
    , key.entityType == "blob"
    , Just fileId <- [Text.stripPrefix (methodName <> ":") key.key]
    ] of
    fileId : _ -> Just fileId
    [] -> Nothing

-- | Animated images have to go through @sendAnimation@; @sendPhoto@ would
-- flatten them to a still frame.
telegramMediaMethod :: BlobType -> ByteString.ByteString -> (Text, ByteString.ByteString)
telegramMediaMethod kind bytes = case kind of
  ImageBlob
    | isAnimatedImage bytes -> ("sendAnimation", "animation")
    | otherwise -> ("sendPhoto", "photo")
  VideoBlob -> ("sendVideo", "video")
  AudioBlob -> ("sendAudio", "audio")
  FileBlob -> ("sendDocument", "document")

telegramSendMediaId :: Context timeline -> Integer -> Maybe Int -> (Text, ByteString.ByteString) -> Text -> IO (Either String TelegramMessage)
telegramSendMediaId context chatId replyTo (methodName, fieldName) fileId = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/" <> methodName))
  let request = urlEncodedBody
        ([("chat_id", ByteString.Char8.pack (show chatId)), (fieldName, Text.encodeUtf8 fileId)]
          <> maybe [] (\messageId -> [("reply_to_message_id", ByteString.Char8.pack (show messageId))]) replyTo)
        request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

telegramSendMedia :: Context timeline -> Integer -> Maybe Int -> (Text, ByteString.ByteString) -> ByteString.ByteString -> IO (Either String TelegramMessage)
telegramSendMedia context chatId replyTo (methodName, fieldName) bytes = do
  let boundary = chooseBoundary "multiverse-telegram" bytes
      field name value = "--" <> boundary <> "\r\nContent-Disposition: form-data; name=\"" <> name <> "\"\r\n\r\n" <> value <> "\r\n"
      body = ByteString.concat
        ( [field "chat_id" (ByteString.Char8.pack (show chatId))]
          <> maybe [] (\messageId -> [field "reply_to_message_id" (ByteString.Char8.pack (show messageId))]) replyTo
          <> [ "--" <> boundary <> "\r\nContent-Disposition: form-data; name=\"" <> fieldName <> "\"; filename=\"upload\"\r\nContent-Type: application/octet-stream\r\n\r\n"
             , bytes, "\r\n--", boundary, "--\r\n" ] )
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/" <> methodName))
  let request = request0 {method = "POST", requestBody = RequestBodyBS body, requestHeaders = [("Content-Type", "multipart/form-data; boundary=" <> boundary)]}
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

chooseBoundary :: ByteString.ByteString -> ByteString.ByteString -> ByteString.ByteString
chooseBoundary boundary bytes
  | boundary `ByteString.isInfixOf` bytes = chooseBoundary (boundary <> "x") bytes
  | otherwise = boundary

telegramHtml :: RenderedMessage -> Text
telegramHtml message =
  telegramSupportedHtml message.htmlText

telegramSupportedHtml :: Text -> Text
telegramSupportedHtml =
  Text.replace "<strong>" "<b>"
    . Text.replace "</strong>" "</b>"
    . Text.replace "<ul>" ""
    . Text.replace "</ul>" ""
    . Text.replace "<li>" "- "
    . Text.replace "</li>" "\n"
    . replaceHeading 1
    . replaceHeading 2
    . replaceHeading 3
    . replaceHeading 4
    . replaceHeading 5
    . replaceHeading 6

replaceHeading :: Int -> Text -> Text
replaceHeading level =
  Text.replace ("<h" <> label <> ">") "<b>"
    . Text.replace ("</h" <> label <> ">") "</b>"
 where
  label = Text.pack (show level)

telegramDeleteMessage :: Context timeline -> Integer -> Int -> IO (Either String ())
telegramDeleteMessage context chatId messageId = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/deleteMessage"))
  let request =
        urlEncodedBody
          [ ("chat_id", ByteString.Char8.pack (show chatId))
          , ("message_id", ByteString.Char8.pack (show messageId))
          ]
          request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

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
telegramGetChat context chatId = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/getChat"))
  let request =
        setQueryString
          [("chat_id", Just (ByteString.Char8.pack (show chatId)))]
          request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeResponse response)

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
telegramMessageKey message =
  PlatformKey "telegram" "message" (Text.pack (show message.chat.chatId <> "/" <> show message.messageId))

-- | @file_unique_id@ is stable across bots while @file_id@ is per-bot, so the
-- stable one identifies the observed file and the fallback only stands in when
-- the server omits it.
telegramSourceKey :: TelegramMedia -> PlatformKey
telegramSourceKey media = PlatformKey "telegram" "blob" (fromMaybe media.fileId media.fileUniqueId)

telegramMessageRefKey :: TelegramMessageRef -> PlatformKey
telegramMessageRefKey message =
  PlatformKey "telegram" "message" (Text.pack (show message.chat.chatId <> "/" <> show message.messageId))

telegramRetractionKey :: EventId -> PlatformKey
telegramRetractionKey eventId_ = PlatformKey "telegram" "retraction" (renderEventId eventId_)

renderEventId :: EventId -> Text
renderEventId (EventId eventId_) = eventId_

telegramUserName :: TelegramUser -> Text
telegramUserName user =
  Text.intercalate " " (filter (not . Text.null) (catMaybes [user.firstName, user.lastName]))

telegramChatName :: TelegramChat -> Text
telegramChatName chat = maybe (Text.pack (show chat.chatId)) id chat.title

telegramMessageBody :: Timeline timeline => Context timeline -> TelegramMessage -> IO (Maybe Message)
telegramMessageBody context message = do
  media <- traverse importMedia message.media
  let textParts = maybe [] (NonEmpty.toList . (`telegramMessageFromEntities` message.entities)) message.text
  pure (nonEmpty (textParts <> maybe [] (\(kind, blob) -> [Blob kind blob]) media))
 where
  importMedia (kind, media) = do
    let sourceKey = telegramSourceKey media
    existing <- context.mappingStore.lookupTimelineId sourceKey
    content <- case existing of
      Just blobEvent -> do
        stored <- getEvent context.timeline blobEvent
        case fmap (.storedEvent.content) stored of
          Just (CreateBlob bytes) -> pure bytes
          _ -> telegramDownloadFile context media.fileId
      Nothing -> telegramDownloadFile context media.fileId
    blob <- case existing of
      Just eventId -> pure (BlobId eventId)
      Nothing -> do
        submitted <- submitBlob context.timeline context.mappingStore sourceKey content
        either (fail . show) pure submitted
    let (methodName, _) = telegramMediaMethod kind content
    context.mappingStore.insertMapping (telegramBlobKey methodName media.fileId) (blobEventId blob)
    pure (kind, blob)

telegramDownloadFile :: Context timeline -> Text -> IO ByteString.ByteString
telegramDownloadFile context fileId = do
  request0 <- parseRequest (Text.unpack (context.apiBase <> "/getFile"))
  response <- loggedHttp context.logger (setQueryString [("file_id", Just (Text.encodeUtf8 fileId))] request0) context.manager
  file <- either fail pure (decodeResponse response :: Either String TelegramFile)
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
