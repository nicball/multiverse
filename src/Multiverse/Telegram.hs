module Multiverse.Telegram
  ( TelegramConfig (..)
  , telegramBridge
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, forever, unless)
import Data.Aeson
import Data.ByteString qualified as ByteString
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.List (sortOn)
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Event hiding (eventId)
import Multiverse.HTTP (HttpLogOptions (..), defaultHttpLogOptions)
import Multiverse.HTTP qualified as HTTP
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data TelegramConfig = TelegramConfig
  { botToken :: Text
  , initialRooms :: [InitialRoomMapping]
  , pollTimeoutSeconds :: Int
  , noteInitialRoom :: InitialRoomMapping -> RoomId -> IO ()
  }

telegramBridge :: Timeline timeline => TelegramConfig -> Bridge timeline
telegramBridge config =
  Bridge
    { bridgeName = "telegram"
    , observe = observeTelegram config
    , reflect = reflectTelegram config
    }

observeTelegram :: Timeline timeline => TelegramConfig -> BridgeContext timeline -> IO ()
observeTelegram config context = do
  manager <- newManager tlsManagerSettings
  ensureInitialRoomsWith
    config.initialRooms
    context
    (fetchInitialRoomInfo context.logger manager config)
    config.noteInitialRoom
  forever do
    offset <- fmap readTextInt <$> context.mappingStore.lookupState "telegram.update_offset"
    response <- telegramGetUpdates context.logger manager config offset
    case response of
      Left err -> do
        logError context.logger ("telegram observe error: " <> Text.pack err)
        threadDelay 5000000
      Right updates -> do
        mapM_ (observeUpdate manager config context) updates
        case updates of
          [] -> pure ()
          _ -> context.mappingStore.setState "telegram.update_offset" (Text.pack (show (maximum (map (.updateId) updates) + 1)))

reflectTelegram :: Timeline timeline => TelegramConfig -> BridgeContext timeline -> IO ()
reflectTelegram config context = do
  manager <- newManager tlsManagerSettings
  forever do
    afterEventId <- fmap (fmap EventId) (context.mappingStore.lookupState "telegram.reflect_event")
    storedEvents <- getEventsAfter context.timeline afterEventId
    mapM_ (reflectEvent manager config context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "telegram.reflect_event" (renderEventId ((last storedEvents).storedId))

observeUpdate :: Timeline timeline => Manager -> TelegramConfig -> BridgeContext timeline -> TelegramUpdate -> IO ()
observeUpdate manager config context update = do
  case update.editedMessage of
    Just message -> observeTelegramEdit manager config context update.updateId message
    Nothing -> pure ()
  case update.message of
    Nothing -> pure ()
    Just message -> do
      room <- configuredRoom config context message.chat
      case room of
        Nothing -> logDebug context.logger ("telegram ignoring unconfigured room " <> (telegramRoomKey message.chat).key)
        Just roomId -> do
          body <- telegramMessageBody manager config context message
          case body of
            Nothing -> pure ()
            Just messageBody -> do
              userId <- ensureUser context message.from
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
              submitted <- submitMessageMapped context event
              case submitted of
                Right _ -> pure ()
                Left err -> fail ("telegram submit error: " <> show err)

observeTelegramEdit :: Timeline timeline => Manager -> TelegramConfig -> BridgeContext timeline -> Int -> TelegramMessage -> IO ()
observeTelegramEdit manager config context updateId message = do
  room <- configuredRoom config context message.chat
  case room of
    Nothing -> pure ()
    Just _ -> do
      target <- context.mappingStore.lookupTimelineId (telegramMessageKey message)
      case target of
        Nothing -> pure ()
        Just targetId -> do
          body <- telegramMessageBody manager config context message
          case body of
            Nothing -> pure ()
            Just newBody -> do
              let key = PlatformKey "telegram" "edit" (Text.pack (show updateId))
              result <- submitMapped context Event {platformKey = key, content = EditMessage (MessageId targetId) newBody}
              either (fail . show) (const (pure ())) result

telegramReplyTo :: BridgeContext timeline -> TelegramMessage -> IO (Maybe MessageId)
telegramReplyTo context message =
  case message.replyToMessage of
    Nothing -> pure Nothing
    Just repliedMessage -> do
      mapped <- context.mappingStore.lookupTimelineId (telegramMessageRefKey repliedMessage)
      pure (MessageId <$> mapped)

ensureUser :: Timeline timeline => BridgeContext timeline -> TelegramUser -> IO UserId
ensureUser context user = do
  let platformKey = telegramUserKey user
  existing <- context.mappingStore.lookupTimelineId platformKey
  case existing of
    Just eventId -> pure (UserId eventId)
    Nothing -> do
      let event = Event {platformKey, content = CreateUser UserInfo {name = telegramUserName user, avatar = Nothing}}
      mapped <- submitMapped context event
      case mapped of
        Right eventId -> pure (UserId eventId)
        Left err -> fail ("could not create telegram user: " <> show err)

configuredRoom :: TelegramConfig -> BridgeContext timeline -> TelegramChat -> IO (Maybe RoomId)
configuredRoom config context chat = do
  let platformKey = telegramRoomKey chat
  if platformKey `elem` map (.platformKey) config.initialRooms
    then fmap RoomId <$> context.mappingStore.lookupTimelineId platformKey
    else pure Nothing

reflectEvent :: Timeline timeline => Manager -> TelegramConfig -> BridgeContext timeline -> StoredEvent -> IO ()
reflectEvent manager config context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "telegram" ->
          logDebug context.logger ("telegram not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
          let telegramRooms = filter ((== "telegram") . (.platform)) roomKeys
          mapM_ (sendToRoom info) telegramRooms
    EditMessage message body
      | stored.storedEvent.platformKey.platform /= "telegram" -> do
          original <- getEvent context.timeline (messageEventId message)
          case fmap (.storedEvent.content) original of
            Just (SendMessage info) -> do
              unless (messageBlobs info.body == messageBlobs body) do
                logWarn context.logger "telegram cannot reflect changes to message attachments"
              targets <- telegramRetractTargets context message
              text <- renderRelayedMessage context info {body = textBody body}
              forM_ targets \(chatId, messageId) -> do
                textId <- context.mappingStore.lookupState ("telegram.send." <> renderEventId (messageEventId message) <> "." <> Text.pack (show chatId) <> ".0")
                whenTextTarget textId messageId do
                  let editKey = "telegram.edit." <> renderEventId stored.storedId <> "." <> Text.pack (show chatId) <> "." <> Text.pack (show messageId)
                  done <- context.mappingStore.lookupState editKey
                  case done of
                    Just _ -> pure ()
                    Nothing -> do
                      result <- telegramEditText context.logger manager config chatId messageId text
                      case result of
                        Left err | not ("message is not modified" `Text.isInfixOf` Text.toLower (Text.pack err)) -> fail ("telegram edit failed: " <> err)
                        _ -> context.mappingStore.setState editKey "done"
            _ -> pure ()
      | otherwise -> pure ()
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "telegram" ->
          logDebug context.logger ("telegram not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          alreadyMapped <- hasPlatformMapping context.mappingStore "telegram" stored.storedId
          unless alreadyMapped do
            targets <- telegramRetractTargets context message
            mapM_ deleteInChat targets
    _ -> pure ()
 where
  sendToRoom info roomKey =
    case readMaybe (Text.unpack roomKey.key) of
      Nothing -> pure ()
      Just chatId -> do
        text <- renderRelayedMessage context info {body = textBody info.body}
        replyTo <- telegramReplyMessageId context roomKey info.replyTo
        let stateKey index = "telegram.send." <> renderEventId stored.storedId <> "." <> roomKey.key <> "." <> Text.pack (show index)
            sendPart index action = do
              sent <- context.mappingStore.lookupState (stateKey index)
              case sent of
                Just _ -> pure ()
                Nothing -> do
                  result <- action
                  message <- either (fail . ("telegram reflect error: " <>)) pure result
                  context.mappingStore.insertMapping (telegramMessageKey message) stored.storedId
                  context.mappingStore.setState (stateKey index) (Text.pack (show message.messageId))
        sendPart (0 :: Int) (telegramSendMessage context.logger manager config chatId replyTo text)
        forM_ (zip [1 ..] (messageBlobs info.body)) \(index, (kind, blobId)) -> do
          blob <- getEvent context.timeline (blobEventId blobId)
          case fmap (.storedEvent.content) blob of
            Just (CreateBlob bytes) -> sendPart index (sendMappedMedia context manager config chatId replyTo kind blobId bytes)
            _ -> fail "telegram media blob is missing"
  deleteInChat (chatId, messageId) = do
    result <- telegramDeleteMessage context.logger manager config chatId messageId
    case result of
      Left err -> logError context.logger ("telegram retract reflect error: " <> Text.pack err)
      Right () -> context.mappingStore.insertMapping (telegramRetractionKey stored.storedId) stored.storedId

whenTextTarget :: Maybe Text -> Int -> IO () -> IO ()
whenTextTarget target messageId action =
  case target of
    Just value | readMaybe (Text.unpack value) /= Just messageId -> pure ()
    _ -> action

textBody :: Message -> Message
textBody body = case nonEmpty (filter isText (NonEmpty.toList body)) of
  Just parts -> parts
  Nothing -> plainMessage ""
 where
  isText (Blob _ _) = False
  isText _ = True

messageBlobs :: Message -> [(BlobType, BlobId)]
messageBlobs = concatMap collect . NonEmpty.toList
 where
  collect = \case
    Blob kind blob -> [(kind, blob)]
    BlockQuote body -> messageBlobs body
    List items -> concatMap messageBlobs (NonEmpty.toList items)
    _ -> []

telegramRetractTargets :: BridgeContext timeline -> MessageId -> IO [(Integer, Int)]
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

telegramReplyMessageId :: BridgeContext timeline -> PlatformKey -> Maybe MessageId -> IO (Maybe Int)
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

telegramGetUpdates :: Logger -> Manager -> TelegramConfig -> Maybe Int -> IO (Either String [TelegramUpdate])
telegramGetUpdates logger manager config offset = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/getUpdates"))
  let query =
        [ ("timeout", Just (ByteString.Char8.pack (show config.pollTimeoutSeconds)))
        , ("allowed_updates", Just "[\"message\",\"edited_message\"]")
        ]
          <> maybe [] (\value -> [("offset", Just (ByteString.Char8.pack (show value)))]) offset
      request = setQueryString query request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

telegramSendMessage :: Logger -> Manager -> TelegramConfig -> Integer -> Maybe Int -> RenderedMessage -> IO (Either String TelegramMessage)
telegramSendMessage logger manager config chatId replyTo text = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/sendMessage"))
  let request =
        urlEncodedBody
          ( [ ("chat_id", ByteString.Char8.pack (show chatId))
            , ("text", Text.encodeUtf8 (telegramHtml text))
            , ("parse_mode", "HTML")
            ]
              <> maybe [] (\messageId -> [("reply_to_message_id", ByteString.Char8.pack (show messageId))]) replyTo
          )
          request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

telegramEditText :: Logger -> Manager -> TelegramConfig -> Integer -> Int -> RenderedMessage -> IO (Either String TelegramMessage)
telegramEditText logger manager config chatId messageId text = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/editMessageText"))
  let request = urlEncodedBody
        [ ("chat_id", ByteString.Char8.pack (show chatId))
        , ("message_id", ByteString.Char8.pack (show messageId))
        , ("text", Text.encodeUtf8 (telegramHtml text))
        , ("parse_mode", "HTML")
        ] request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

sendMappedMedia :: BridgeContext timeline -> Manager -> TelegramConfig -> Integer -> Maybe Int -> BlobType -> BlobId -> ByteString.ByteString -> IO (Either String TelegramMessage)
sendMappedMedia context manager config chatId replyTo kind blobId bytes = do
  let stateKey = telegramBlobStateKey kind blobId
      upload = telegramSendMedia context.logger manager config chatId replyTo kind bytes
  existing <- context.mappingStore.lookupState stateKey
  result <- case existing of
    Nothing -> upload
    Just fileId -> do
      sent <- telegramSendMediaId context.logger manager config chatId replyTo kind fileId
      case sent of
        Right message -> pure (Right message)
        Left err -> do
          logWarn context.logger ("telegram file ID rejected; uploading again: " <> Text.pack err)
          upload
  case result of
    Right message -> case message.media of
      Just (returnedKind, fileId) | returnedKind == kind -> do
        context.mappingStore.insertMapping (PlatformKey "telegram" "blob" fileId) (blobEventId blobId)
        context.mappingStore.setState stateKey fileId
      _ -> pure ()
    Left _ -> pure ()
  pure result

telegramBlobStateKey :: BlobType -> BlobId -> Text
telegramBlobStateKey kind blobId =
  "telegram.blob." <> renderEventId (blobEventId blobId) <> "." <> Text.pack (show kind)

telegramMediaMethod :: BlobType -> (Text, ByteString.ByteString)
telegramMediaMethod = \case
  ImageBlob -> ("sendPhoto", "photo")
  VideoBlob -> ("sendVideo", "video")
  AudioBlob -> ("sendAudio", "audio")
  FileBlob -> ("sendDocument", "document")

telegramSendMediaId :: Logger -> Manager -> TelegramConfig -> Integer -> Maybe Int -> BlobType -> Text -> IO (Either String TelegramMessage)
telegramSendMediaId logger manager config chatId replyTo kind fileId = do
  let (methodName, fieldName) = telegramMediaMethod kind
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/" <> methodName))
  let request = urlEncodedBody
        ([("chat_id", ByteString.Char8.pack (show chatId)), (fieldName, Text.encodeUtf8 fileId)]
          <> maybe [] (\messageId -> [("reply_to_message_id", ByteString.Char8.pack (show messageId))]) replyTo)
        request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

telegramSendMedia :: Logger -> Manager -> TelegramConfig -> Integer -> Maybe Int -> BlobType -> ByteString.ByteString -> IO (Either String TelegramMessage)
telegramSendMedia logger manager config chatId replyTo kind bytes = do
  let (methodName, fieldName) = telegramMediaMethod kind
      boundary = chooseBoundary "multiverse-telegram" bytes
      field name value = "--" <> boundary <> "\r\nContent-Disposition: form-data; name=\"" <> name <> "\"\r\n\r\n" <> value <> "\r\n"
      body = ByteString.concat
        ( [field "chat_id" (ByteString.Char8.pack (show chatId))]
          <> maybe [] (\messageId -> [field "reply_to_message_id" (ByteString.Char8.pack (show messageId))]) replyTo
          <> [ "--" <> boundary <> "\r\nContent-Disposition: form-data; name=\"" <> fieldName <> "\"; filename=\"upload\"\r\nContent-Type: application/octet-stream\r\n\r\n"
             , bytes, "\r\n--", boundary, "--\r\n" ] )
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/" <> methodName))
  let request = request0 {method = "POST", requestBody = RequestBodyBS body, requestHeaders = [("Content-Type", "multipart/form-data; boundary=" <> boundary)]}
  response <- loggedHttp logger request manager
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

telegramDeleteMessage :: Logger -> Manager -> TelegramConfig -> Integer -> Int -> IO (Either String ())
telegramDeleteMessage logger manager config chatId messageId = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/deleteMessage"))
  let request =
        urlEncodedBody
          [ ("chat_id", ByteString.Char8.pack (show chatId))
          , ("message_id", ByteString.Char8.pack (show messageId))
          ]
          request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

fetchInitialRoomInfo :: Logger -> Manager -> TelegramConfig -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo logger manager config mapping =
  case readMaybe (Text.unpack mapping.platformKey.key) of
    Nothing -> fail ("telegram room key is not a chat id: " <> Text.unpack mapping.platformKey.key)
    Just chatId -> do
      result <- telegramGetChat logger manager config chatId
      case result of
        Left err -> fail ("telegram getChat failed: " <> err)
        Right chat -> pure RoomInfo {name = telegramChatName chat, description = "", avatar = Nothing}

telegramGetChat :: Logger -> Manager -> TelegramConfig -> Integer -> IO (Either String TelegramChat)
telegramGetChat logger manager config chatId = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/getChat"))
  let request =
        setQueryString
          [("chat_id", Just (ByteString.Char8.pack (show chatId)))]
          request0
  response <- loggedHttp logger request manager
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

telegramMessageBody :: Timeline timeline => Manager -> TelegramConfig -> BridgeContext timeline -> TelegramMessage -> IO (Maybe Message)
telegramMessageBody manager config context message = do
  media <- traverse importMedia message.media
  let textParts = maybe [] (NonEmpty.toList . (`telegramMessageFromEntities` message.entities)) message.text
  pure (nonEmpty (textParts <> maybe [] (\(kind, blob) -> [Blob kind blob]) media))
 where
  importMedia (kind, fileId) = do
    let key = PlatformKey "telegram" "blob" fileId
    existing <- context.mappingStore.lookupTimelineId key
    blobId <- case existing of
      Just value -> pure value
      Nothing -> do
        bytes <- telegramDownloadFile context.logger manager config fileId
        submitted <- submitMapped context Event {platformKey = key, content = CreateBlob bytes}
        either (fail . show) pure submitted
    let blob = BlobId blobId
    context.mappingStore.setState (telegramBlobStateKey kind blob) fileId
    pure (kind, blob)

telegramDownloadFile :: Logger -> Manager -> TelegramConfig -> Text -> IO ByteString.ByteString
telegramDownloadFile logger manager config fileId = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/getFile"))
  response <- loggedHttp logger (setQueryString [("file_id", Just (Text.encodeUtf8 fileId))] request0) manager
  file <- either fail pure (decodeResponse response :: Either String TelegramFile)
  path <- maybe (fail "telegram getFile response lacks file_path") pure file.filePath
  request <- parseRequest (Text.unpack ("https://api.telegram.org/file/bot" <> config.botToken <> "/" <> path))
  download <- loggedHttp logger request manager
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

plainMessage :: Text -> Message
plainMessage text = Text (Plain text :| []) :| []

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
  , media :: Maybe (BlobType, Text)
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
    let fileId (TelegramMedia value) = value
    TelegramMessage
      <$> obj .: "message_id"
      <*> obj .: "from"
      <*> obj .: "chat"
      <*> ((<|>) <$> obj .:? "text" <*> obj .:? "caption")
      <*> ((maybe [] id) <$> ((<|>) <$> obj .:? "entities" <*> obj .:? "caption_entities"))
      <*> obj .:? "reply_to_message"
      <*> pure ((ImageBlob,) . fileId <$> (photo <|> sticker) <|> (VideoBlob,) . fileId <$> (video <|> videoNote <|> animation) <|> (AudioBlob,) . fileId <$> (audio <|> voice) <|> (FileBlob,) . fileId <$> document)

newtype TelegramMedia = TelegramMedia Text

instance FromJSON TelegramMedia where
  parseJSON = withObject "TelegramMedia" \obj -> TelegramMedia <$> obj .: "file_id"

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
  , username :: Maybe Text
  , firstName :: Maybe Text
  , lastName :: Maybe Text
  }

instance FromJSON TelegramUser where
  parseJSON = withObject "TelegramUser" \obj ->
    TelegramUser
      <$> obj .: "id"
      <*> obj .:? "username"
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
