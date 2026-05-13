module Multiverse.Telegram
  ( TelegramConfig (..)
  , telegramBridge
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (try)
import Control.Monad (forever, unless)
import Data.Aeson
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.Text.Encoding.Error (lenientDecode)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.List.NonEmpty (toList)
import Data.Maybe (catMaybes)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types (statusCode)
import Text.Read (readMaybe)
import Multiverse.Bridge
import Multiverse.Event hiding (eventId)
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
        mapM_ (observeUpdate config context) updates
        case updates of
          [] -> pure ()
          _ -> context.mappingStore.setState "telegram.update_offset" (Text.pack (show (maximum (map (.updateId) updates) + 1)))

reflectTelegram :: Timeline timeline => TelegramConfig -> BridgeContext timeline -> IO ()
reflectTelegram config context = do
  manager <- newManager tlsManagerSettings
  forever do
    afterSeq <- fmap (>>= readTimelineSeq) (context.mappingStore.lookupState "telegram.reflect_seq")
    storedEvents <- getEventsAfter context.timeline afterSeq
    mapM_ (reflectEvent manager config context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "telegram.reflect_seq" (Text.pack (show (maximum (map (.storedSeq) storedEvents))))

observeUpdate :: Timeline timeline => TelegramConfig -> BridgeContext timeline -> TelegramUpdate -> IO ()
observeUpdate config context update =
  case update.message >>= telegramMessageText of
    Nothing -> pure ()
    Just (message, body) -> do
      room <- configuredRoom config context message.chat
      case room of
        Nothing -> logDebug context.logger ("telegram ignoring unconfigured room " <> (telegramRoomKey message.chat).key)
        Just roomId -> do
          userId <- ensureUser context message.from
          let platformKey = telegramMessageKey message
              event =
                Event
                  { platformKey
                  , content =
                      SendMessage
                        SendMessageInfo
                          { sender = userId
                          , room = roomId
                          , replyTo = Nothing
                          , forwardOf = Nothing
                          , body = plainMessage body
                          }
                  }
          submitted <- submit context.timeline event
          case submitted of
            Right eventId -> context.mappingStore.insertMapping platformKey eventId
            Left (ConflictingPlatformKey _ eventId) -> context.mappingStore.insertMapping platformKey eventId
            Left err -> logError context.logger ("telegram submit error: " <> Text.pack (show err))

ensureUser :: Timeline timeline => BridgeContext timeline -> TelegramUser -> IO UserId
ensureUser context user = do
  let platformKey = telegramUserKey user
  existing <- context.mappingStore.lookupTimelineId platformKey
  case existing of
    Just eventId -> pure (UserId eventId)
    Nothing -> do
      let event = Event {platformKey, content = CreateUser UserInfo {name = telegramUserName user, avatar = Nothing}}
      submitted <- submit context.timeline event
      case submitted of
        Right eventId -> context.mappingStore.insertMapping platformKey eventId >> pure (UserId eventId)
        Left (ConflictingPlatformKey _ eventId) -> context.mappingStore.insertMapping platformKey eventId >> pure (UserId eventId)
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
          alreadyMapped <- hasPlatformMapping context.mappingStore "telegram" stored.storedId
          unless alreadyMapped do
            roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
            let telegramRooms = filter ((== "telegram") . (.platform)) roomKeys
            mapM_ (sendToRoom info) telegramRooms
    _ -> pure ()
 where
  sendToRoom info roomKey =
    case readMaybe (Text.unpack roomKey.key) of
      Nothing -> pure ()
      Just chatId -> do
        text <- renderRelayedMessage context info
        result <- telegramSendMessage context.logger manager config chatId text
        case result of
          Left err -> logError context.logger ("telegram reflect error: " <> Text.pack err)
          Right message -> context.mappingStore.insertMapping (telegramMessageKey message) stored.storedId

hasPlatformMapping :: MappingStore -> Text -> EventId -> IO Bool
hasPlatformMapping store platform eventId = any ((== platform) . (.platform)) <$> store.lookupPlatformKeys eventId

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

telegramGetUpdates :: Logger -> Manager -> TelegramConfig -> Maybe Int -> IO (Either String [TelegramUpdate])
telegramGetUpdates logger manager config offset = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/getUpdates"))
  let query =
        [ ("timeout", Just (ByteString.Char8.pack (show config.pollTimeoutSeconds)))
        , ("allowed_updates", Just "[\"message\"]")
        ]
          <> maybe [] (\value -> [("offset", Just (ByteString.Char8.pack (show value)))]) offset
      request = setQueryString query request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

telegramSendMessage :: Logger -> Manager -> TelegramConfig -> Integer -> Text -> IO (Either String TelegramMessage)
telegramSendMessage logger manager config chatId text = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/sendMessage"))
  let request =
        urlEncodedBody
          [ ("chat_id", ByteString.Char8.pack (show chatId))
          , ("text", Text.encodeUtf8 text)
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

redactTelegramUrl :: Text -> Text
redactTelegramUrl url =
  case Text.breakOn "/bot" url of
    (prefix, rest)
      | Text.null rest -> url
      | otherwise ->
          let suffix = Text.dropWhile (/= '/') (Text.drop 4 rest)
           in prefix <> "/bot<redacted>" <> suffix

requestUrl :: Request -> Text
requestUrl request =
  redactTelegramUrl
    ( Text.decodeUtf8 (securePrefix request)
        <> Text.decodeUtf8 request.host
        <> portText request
        <> Text.decodeUtf8 request.path
        <> Text.decodeUtf8 request.queryString
    )

securePrefix :: Request -> ByteString.Char8.ByteString
securePrefix request = if request.secure then "https://" else "http://"

portText :: Request -> Text
portText request =
  let defaultPort = if request.secure then 443 else 80
   in if request.port == defaultPort then "" else ":" <> Text.pack (show request.port)

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

renderEventId :: EventId -> Text
renderEventId (EventId eventId_) = eventId_

telegramUserName :: TelegramUser -> Text
telegramUserName user =
  Text.intercalate " " (filter (not . Text.null) (catMaybes [user.firstName, user.lastName]))

telegramChatName :: TelegramChat -> Text
telegramChatName chat = maybe (Text.pack (show chat.chatId)) id chat.title

telegramMessageText :: TelegramMessage -> Maybe (TelegramMessage, Text)
telegramMessageText message = fmap (\body -> (message, body)) message.text

plainMessage :: Text -> Message
plainMessage text = Text (Plain text :| []) :| []

readTextInt :: Text -> Int
readTextInt = maybe 0 id . readMaybe . Text.unpack

readTimelineSeq :: Text -> Maybe TimelineSeq
readTimelineSeq = readMaybe . Text.unpack

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
  }

instance FromJSON TelegramUpdate where
  parseJSON = withObject "TelegramUpdate" \obj ->
    TelegramUpdate
      <$> obj .: "update_id"
      <*> obj .:? "message"

data TelegramMessage = TelegramMessage
  { messageId :: Int
  , from :: TelegramUser
  , chat :: TelegramChat
  , text :: Maybe Text
  }

instance FromJSON TelegramMessage where
  parseJSON = withObject "TelegramMessage" \obj ->
    TelegramMessage
      <$> obj .: "message_id"
      <*> obj .: "from"
      <*> obj .: "chat"
      <*> obj .:? "text"

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
