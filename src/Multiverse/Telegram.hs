module Multiverse.Telegram
  ( TelegramConfig (..)
  , telegramBridge
  )
where

import Control.Concurrent (threadDelay)
import Control.Monad (forever, unless)
import Data.Aeson
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.List.NonEmpty (NonEmpty ((:|)))
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
        mapM_ (observeUpdate config context) updates
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
                          , body = plainMessage body
                          }
                  }
          submitted <- submitMessageMapped context event
          case submitted of
            Right _ -> pure ()
            Left err -> logError context.logger ("telegram submit error: " <> Text.pack (show err))

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
          alreadyMapped <- hasPlatformMapping context.mappingStore "telegram" stored.storedId
          unless alreadyMapped do
            roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
            let telegramRooms = filter ((== "telegram") . (.platform)) roomKeys
            mapM_ (sendToRoom info) telegramRooms
    RetractMessage _ message
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
        text <- renderRelayedMessage context info
        replyTo <- telegramReplyMessageId context roomKey info.replyTo
        result <- telegramSendMessage context.logger manager config chatId replyTo text
        case result of
          Left err -> logError context.logger ("telegram reflect error: " <> Text.pack err)
          Right message -> context.mappingStore.insertMapping (telegramMessageKey message) stored.storedId
  deleteInChat (chatId, messageId) = do
    result <- telegramDeleteMessage context.logger manager config chatId messageId
    case result of
      Left err -> logError context.logger ("telegram retract reflect error: " <> Text.pack err)
      Right () -> context.mappingStore.insertMapping (telegramRetractionKey stored.storedId) stored.storedId

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
        , ("allowed_updates", Just "[\"message\"]")
        ]
          <> maybe [] (\value -> [("offset", Just (ByteString.Char8.pack (show value)))]) offset
      request = setQueryString query request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

telegramSendMessage :: Logger -> Manager -> TelegramConfig -> Integer -> Maybe Int -> Text -> IO (Either String TelegramMessage)
telegramSendMessage logger manager config chatId replyTo text = do
  request0 <- parseRequest (Text.unpack ("https://api.telegram.org/bot" <> config.botToken <> "/sendMessage"))
  let request =
        urlEncodedBody
          ( [ ("chat_id", ByteString.Char8.pack (show chatId))
            , ("text", Text.encodeUtf8 text)
            ]
              <> maybe [] (\messageId -> [("reply_to_message_id", ByteString.Char8.pack (show messageId))]) replyTo
          )
          request0
  response <- loggedHttp logger request manager
  pure (decodeResponse response)

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

telegramMessageText :: TelegramMessage -> Maybe (TelegramMessage, Text)
telegramMessageText message = fmap (\body -> (message, body)) message.text

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
  , replyToMessage :: Maybe TelegramMessageRef
  }

instance FromJSON TelegramMessage where
  parseJSON = withObject "TelegramMessage" \obj ->
    TelegramMessage
      <$> obj .: "message_id"
      <*> obj .: "from"
      <*> obj .: "chat"
      <*> obj .:? "text"
      <*> obj .:? "reply_to_message"

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
