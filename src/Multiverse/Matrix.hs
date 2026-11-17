module Multiverse.Matrix
  ( MatrixConfig (..)
  , matrixBridge
  )
where

import Control.Concurrent (threadDelay)
import Control.Applicative (asum, (<|>))
import Control.Exception (try)
import Control.Monad (forM, forM_, forever, unless)
import Data.Aeson
import Data.Aeson.Types (Parser)
import Data.Aeson.Key qualified as Aeson.Key
import Data.Aeson.KeyMap qualified as Aeson.KeyMap
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LByteString
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty)
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types.Status (statusCode)
import Network.HTTP.Types.URI (urlEncode)
import Text.HTML.TagSoup
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
    afterEventId <- fmap (fmap EventId) (context.mappingStore.lookupState "matrix.reflect_event")
    storedEvents <- getEventsAfter context.timeline afterEventId
    mapM_ (reflectEvent manager config context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "matrix.reflect_event" (renderEventId ((last storedEvents).storedId))

observeRoomEvents :: Timeline timeline => Manager -> MatrixConfig -> BridgeContext timeline -> Text -> MatrixRoomEvents -> IO ()
observeRoomEvents manager config context ownUserId roomEvents = do
  room <- configuredRoom config context roomEvents.roomId
  case room of
    Nothing -> logDebug context.logger ("matrix ignoring unconfigured room " <> roomEvents.roomId)
    Just roomId -> mapM_ (observeEvent manager context config ownUserId roomId) roomEvents.events

observeEvent :: Timeline timeline => Manager -> BridgeContext timeline -> MatrixConfig -> Text -> RoomId -> MatrixEvent -> IO ()
observeEvent manager context config ownUserId room event
  | event.sender == ownUserId, not (isMemberEvent event.content) =
      logDebug context.logger ("matrix ignoring own event " <> event.eventId)
  | otherwise =
      case event.content of
        MatrixMessage messageContent -> observeMatrixMessage manager context config event room messageContent
        MatrixMedia mediaContent -> observeMatrixMedia manager context config event room mediaContent
        MatrixRedaction redactedEvent -> do
          target <- context.mappingStore.lookupTimelineId (matrixEventKey redactedEvent)
          case target of
            Nothing -> logDebug context.logger ("matrix ignoring redaction for unknown event " <> redactedEvent)
            Just targetEventId -> do
              let timelineEvent =
                    Event
                      { platformKey = matrixRedactionKey event.eventId
                      , content = RetractMessage (MessageId targetEventId)
                      }
              submitted <- submitMapped context timelineEvent
              case submitted of
                Right _ -> pure ()
                Left (MissingReferences missing)
                  | targetEventId `elem` missing ->
                      logDebug context.logger ("matrix ignoring redaction for missing timeline event " <> renderEventId targetEventId)
                Left err -> logError context.logger ("matrix retraction submit error: " <> Text.pack (show err))
        MatrixMember member -> observeMatrixMember manager context config ownUserId room event member
        MatrixIgnored reason
          | reason == "m.room.encrypted" ->
              logWarn context.logger ("matrix cannot decrypt event " <> event.eventId <> ": encrypted rooms are not supported")
          | otherwise -> logDebug context.logger ("matrix ignoring " <> reason <> " event " <> event.eventId)

observeMatrixMessage :: Timeline timeline => Manager -> BridgeContext timeline -> MatrixConfig -> MatrixEvent -> RoomId -> MatrixMessageContent -> IO ()
observeMatrixMessage manager context config event room messageContent = do
  userId <- ensureUser manager context config event.sender
  ensureRoomMembership context "matrix" room userId
  case messageContent.messageEditOf of
    Just targetEventId -> do
      target <- context.mappingStore.lookupTimelineId (matrixEventKey targetEventId)
      case target of
        Nothing -> logDebug context.logger ("matrix ignoring edit for unknown event " <> targetEventId)
        Just targetMessage -> do
          let timelineEvent =
                Event
                  { platformKey = PlatformKey "matrix" "edit" event.eventId
                  , content = EditMessage (MessageId targetMessage) messageContent.messageBody
                  }
          submitted <- submitMapped context timelineEvent
          case submitted of
            Right _ -> pure ()
            Left err -> logError context.logger ("matrix edit submit error: " <> Text.pack (show err))
    Nothing -> do
      replyTo <- matrixReplyTo context messageContent.messageReplyTo
      let timelineEvent =
            Event
              { platformKey = matrixEventKey event.eventId
              , content =
                  SendMessage
                    SendMessageInfo
                      { sender = userId
                      , room
                      , replyTo
                      , forwardOf = Nothing
                      , body = messageContent.messageBody
                      }
              }
      submitted <- submitMessageMapped context timelineEvent
      case submitted of
        Right _ -> pure ()
        Left err -> logError context.logger ("matrix submit error: " <> Text.pack (show err))

observeMatrixMedia :: Timeline timeline => Manager -> BridgeContext timeline -> MatrixConfig -> MatrixEvent -> RoomId -> MatrixMediaContent -> IO ()
observeMatrixMedia manager context config event room mediaContent = do
  userId <- ensureUser manager context config event.sender
  ensureRoomMembership context "matrix" room userId
  let blobKey = PlatformKey "matrix" "blob" event.eventId
  existing <- context.mappingStore.lookupTimelineId blobKey
  blobId <- case existing of
    Just blobEvent -> pure (Just (BlobId blobEvent))
    Nothing -> do
      downloaded <- matrixDownloadMedia context.logger manager config mediaContent.mediaUri
      case downloaded of
        Nothing -> pure Nothing
        Just bytes -> do
          submitted <- submitMapped context Event {platformKey = blobKey, content = CreateBlob bytes}
          case submitted of
            Right blobEvent -> pure (Just (BlobId blobEvent))
            Left err -> do
              logError context.logger ("matrix media submit error: " <> Text.pack (show err))
              pure Nothing
  let label = fromMaybe (matrixMediaLabel mediaContent.mediaKind) mediaContent.mediaName
      parts = case blobId of
        Just blob
          | mediaContent.mediaKind == FileBlob -> [Text (Plain label :| []), Blob FileBlob blob]
          | otherwise -> [Blob mediaContent.mediaKind blob]
        Nothing -> [Text (Plain ("[" <> label <> "]") :| [])]
  case nonEmpty parts of
    Nothing -> pure ()
    Just body -> do
      replyTo <- matrixReplyTo context mediaContent.mediaReplyTo
      let timelineEvent =
            Event
              { platformKey = matrixEventKey event.eventId
              , content =
                  SendMessage
                    SendMessageInfo
                      { sender = userId
                      , room
                      , replyTo
                      , forwardOf = Nothing
                      , body
                      }
              }
      submitted <- submitMessageMapped context timelineEvent
      case submitted of
        Right _ -> pure ()
        Left err -> logError context.logger ("matrix media message submit error: " <> Text.pack (show err))

matrixMediaLabel :: BlobType -> Text
matrixMediaLabel = \case
  ImageBlob -> "image"
  VideoBlob -> "video"
  AudioBlob -> "audio"
  FileBlob -> "file"


isMemberEvent :: MatrixEventContent -> Bool
isMemberEvent content = case content of
  MatrixMember _ -> True
  _ -> False

observeMatrixMember :: Timeline timeline => Manager -> BridgeContext timeline -> MatrixConfig -> Text -> RoomId -> MatrixEvent -> MatrixMembership -> IO ()
observeMatrixMember manager context config ownUserId room event member
  | member.memberUserId == ownUserId =
      logDebug context.logger ("matrix ignoring own membership " <> member.memberUserId)
  | otherwise = case member.membership of
      "join" -> withUser \user -> do
        submitMembership "join" user
        case (member.memberPreviousDisplayName, member.memberDisplayName) of
          (Just previous, Just nick)
            | nick /= previous -> submitNick user nick
          _ -> pure ()
      "leave" -> withUser (submitMembership "leave")
      "ban" -> withUser (submitMembership "leave")
      _ -> pure ()
 where
  withUser action = do
    user <- ensureUser manager context config member.memberUserId
    action user

  eventKey kind = PlatformKey "matrix" kind (event.eventId <> "/" <> member.memberUserId)

  submitEvent platformKey content = do
    result <- submitMapped context Event {platformKey, content}
    case result of
      Left err -> logError context.logger ("matrix membership submit error: " <> Text.pack (show err))
      Right _ -> pure ()

  submitMembership kind user =
    submitEvent (eventKey kind) (if kind == "join" then JoinRoom user room else LeaveRoom user room)

  submitNick user nick = submitEvent (eventKey "nick") (ChangeRoomNick user room nick)

matrixReplyTo :: BridgeContext timeline -> Maybe Text -> IO (Maybe MessageId)
matrixReplyTo context replyToEvent =
  case replyToEvent of
    Nothing -> pure Nothing
    Just repliedEvent -> do
      mapped <- context.mappingStore.lookupTimelineId (matrixEventKey repliedEvent)
      pure (MessageId <$> mapped)

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
  if enabledRoom config.initialRooms platformKey
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
            let matrixRooms = filter (enabledRoom config.initialRooms) roomKeys
            (profile, body) <- renderMatrixRelayMessage context info {body = textBody info.body}
            replyTo <- matrixReplyEventId context info.replyTo
            mapM_ (sendToRoom info profile body replyTo) matrixRooms
    EditMessage message body
      | stored.storedEvent.platformKey.platform /= "matrix" -> do
          original <- getEvent context.timeline (messageEventId message)
          case fmap (.storedEvent.content) original of
            Just (SendMessage info) -> do
              unless (messageBlobs info.body == messageBlobs body) do
                logWarn context.logger "matrix cannot reflect changes to message attachments"
              text <- renderRelayedMessage context info {body = textBody body}
              targets <- matrixEditTargets config context message
              forM_ targets \(roomId, matrixEventId) -> do
                let txnId = matrixTransactionId stored "edit" roomId
                result <- matrixSendEdit context.logger manager config roomId txnId matrixEventId text
                case result of
                  Left err -> logError context.logger ("matrix edit reflect error: " <> Text.pack err)
                  Right editEventId -> context.mappingStore.insertMapping (matrixEventKey editEventId) stored.storedId
            _ -> pure ()
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          alreadyMapped <- hasPlatformMapping context.mappingStore "matrix" stored.storedId
          unless alreadyMapped do
            targets <- matrixRetractTargets context message
            mapM_ redactInRoom (filter (enabledRoom config.initialRooms . matrixRoomKey . fst) targets)
    ChangeRoomNick user room nick
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin nick change " <> renderEventId stored.storedId)
      | otherwise -> do
          body <- renderNickChange context user nick
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
          forM_ (filter (enabledRoom config.initialRooms) roomKeys) \roomKey -> do
            let stateKey = "matrix.nick." <> renderEventId stored.storedId <> "." <> roomKey.key
            done <- context.mappingStore.lookupState stateKey
            case done of
              Just _ -> pure ()
              Nothing -> do
                let txnId = matrixTransactionId stored "nick" roomKey.key
                result <- matrixSendMessage context.logger manager config roomKey.key txnId Nothing Nothing body
                case result of
                  Left err -> logError context.logger ("matrix nick reflect error: " <> Text.pack err)
                  Right matrixEventId -> do
                    context.mappingStore.insertMapping (matrixEventKey matrixEventId) stored.storedId
                    context.mappingStore.setState stateKey "done"
    _ -> pure ()
 where
  sendToRoom info profile body replyTo roomKey = do
    let roomId = roomKey.key
        blobs = messageBlobs info.body
        sendText = do
          let txnId = matrixTransactionId stored "text" roomId
          result <- matrixSendMessage context.logger manager config roomId txnId replyTo (Just profile) body
          case result of
            Left err -> pure (Left err)
            Right matrixEventId -> do
              context.mappingStore.setState ("matrix.text." <> renderEventId stored.storedId <> "." <> roomId) matrixEventId
              pure (Right matrixEventId)
        sendBlob index (kind, blobId) = do
          blob <- getEvent context.timeline (blobEventId blobId)
          case fmap (.storedEvent.content) blob of
            Just (CreateBlob bytes) -> do
              let name = "file-" <> Text.take 12 (renderEventId (blobEventId blobId)) <> sniffExtension bytes
                  mimeType = sniffMimeType bytes
              uploaded <- matrixUploadMedia context.logger manager config name mimeType bytes
              case uploaded of
                Left err -> pure (Left err)
                Right mediaUrl -> do
                  let blobReplyTo = if index == 1 && not (hasTextContent info.body) then replyTo else Nothing
                      content = matrixMediaContent kind name mimeType (ByteString.length bytes) mediaUrl
                      txnId = matrixTransactionId stored ("part" <> Text.pack (show index)) roomId
                  result <- matrixSendMedia context.logger manager config roomId txnId blobReplyTo content
                  case result of
                    Left err -> pure (Left err)
                    Right matrixEventId -> do
                      context.mappingStore.setState ("matrix.room." <> matrixEventId) roomId
                      pure (Right matrixEventId)
            _ -> pure (Left "matrix media blob is missing")
    textResults <-
      if hasTextContent info.body
        then (: []) <$> sendText
        else pure []
    blobResults <- mapM (uncurry sendBlob) (zip [1 :: Int ..] blobs)
    let results = textResults <> blobResults
        errors = [err | Left err <- results]
    if null errors
      then forM_ [matrixEventId | Right matrixEventId <- results] \matrixEventId ->
        context.mappingStore.insertMapping (matrixEventKey matrixEventId) stored.storedId
      else mapM_ (\err -> logError context.logger ("matrix reflect error: " <> Text.pack err)) errors
  redactInRoom (roomId, eventId_) = do
    let txnId = matrixTransactionId stored "redact" roomId
    result <- matrixRedactMessage context.logger manager config roomId txnId eventId_
    case result of
      Left err -> do
        logError context.logger ("matrix retract reflect error: " <> Text.pack err)
        let fallbackTxnId = matrixTransactionId stored "redact-failure" roomId
        fallback <- matrixSendMessage context.logger manager config roomId fallbackTxnId Nothing Nothing (retractionFailureMessage err)
        case fallback of
          Left fallbackErr -> logError context.logger ("matrix retract failure notice error: " <> Text.pack fallbackErr)
          Right matrixEventId -> context.mappingStore.insertMapping (matrixEventKey matrixEventId) stored.storedId
      Right redactionEventId -> context.mappingStore.insertMapping (matrixRedactionKey redactionEventId) stored.storedId

-- | Transaction ids are idempotency keys scoped to the access token, so they
-- must be unique per room and per part.
matrixTransactionId :: StoredEvent -> Text -> Text -> Text
matrixTransactionId stored suffix roomId =
  escapePathSegment (renderEventId stored.storedId <> "-" <> suffix <> "-" <> roomId)

retractionFailureMessage :: String -> RenderedMessage
retractionFailureMessage err =
  let text = "Failed to retract a bridged message: " <> Text.pack err
   in RenderedMessage text text

matrixEditTargets :: Timeline timeline => MatrixConfig -> BridgeContext timeline -> MessageId -> IO [(Text, Text)]
matrixEditTargets config context message = do
  original <- getEvent context.timeline (messageEventId message)
  case fmap (.storedEvent.content) original of
    Just (SendMessage info) -> do
      roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
      let matrixRooms = filter (enabledRoom config.initialRooms) roomKeys
      sent <- forM matrixRooms \roomKey -> do
        sentEvent <- context.mappingStore.lookupState ("matrix.text." <> renderEventId (messageEventId message) <> "." <> roomKey.key)
        pure (fmap (\matrixEventId -> (roomKey.key, matrixEventId)) sentEvent)
      case catMaybes sent of
        [] -> do
          -- The message was observed, not sent, so its event id is mapped directly.
          messageKeys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
          let matrixEvents =
                [ platformKey.key
                | platformKey <- messageKeys
                , platformKey.platform == "matrix"
                , platformKey.entityType == "message"
                ]
          pure [(roomKey.key, matrixEventId) | roomKey <- matrixRooms, matrixEventId <- matrixEvents]
        targets -> pure targets
    _ -> pure []

matrixRetractTargets :: Timeline timeline => BridgeContext timeline -> MessageId -> IO [(Text, Text)]
matrixRetractTargets context message = do
  original <- getEvent context.timeline (messageEventId message)
  case fmap (.storedEvent.content) original of
    Just (SendMessage info) -> do
      messageKeys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
      let matrixEvents =
            [ platformKey.key
            | platformKey <- messageKeys
            , platformKey.platform == "matrix"
            , platformKey.entityType == "message"
            ]
      targets <- forM matrixEvents \matrixEventId -> do
        recordedRoom <- context.mappingStore.lookupState ("matrix.room." <> matrixEventId)
        case recordedRoom of
          Just roomId -> pure [(roomId, matrixEventId)]
          Nothing -> do
            roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
            pure
              [ (roomKey.key, matrixEventId)
              | roomKey <- roomKeys
              , roomKey.platform == "matrix"
              , roomKey.entityType == "room"
              ]
      pure (concat targets)
    _ -> pure []

matrixReplyEventId :: BridgeContext timeline -> Maybe MessageId -> IO (Maybe Text)
matrixReplyEventId _ Nothing = pure Nothing
matrixReplyEventId context (Just message) = do
  keys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
  pure case [platformKey.key | platformKey <- keys, platformKey.platform == "matrix", platformKey.entityType == "message"] of
    eventId_ : _ -> Just eventId_
    [] -> Nothing

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

renderMatrixRelayMessage :: Timeline timeline => BridgeContext timeline -> SendMessageInfo -> IO (Value, RenderedMessage)
renderMatrixRelayMessage context info = do
  user <- getUserInfo context.timeline info.sender (Just info.room)
  let displayName = matrixProfileDisplayName (maybe "unknown" (.name) user)
      message = renderMessageBody info.body
      profile =
        object
          [ "id" .= renderEventId (userEventId info.sender)
          , "displayname" .= displayName
          , "avatar_url" .= ("" :: Text)
          , "has_fallback" .= True
          ]
      fallback =
        RenderedMessage
          { plainText = displayName <> ": " <> message.plainText
          , htmlText =
              "<strong data-mx-profile-fallback>"
                <> matrixHtmlEscape displayName
                <> ": </strong>"
                <> message.htmlText
          }
  pure (profile, fallback)

matrixProfileDisplayName :: Text -> Text
matrixProfileDisplayName name =
  let bounded = truncateUtf8 255 (Text.filter (/= '\0') name)
   in if Text.null bounded then "unknown" else bounded

truncateUtf8 :: Int -> Text -> Text
truncateUtf8 limit = Text.pack . go 0 . Text.unpack
 where
  go _ [] = []
  go bytes (char : rest)
    | nextBytes > limit = []
    | otherwise = char : go nextBytes rest
   where
    nextBytes = bytes + ByteString.length (Text.encodeUtf8 (Text.singleton char))

matrixHtmlEscape :: Text -> Text
matrixHtmlEscape =
  Text.concatMap \case
    '&' -> "&amp;"
    '<' -> "&lt;"
    '>' -> "&gt;"
    '"' -> "&quot;"
    '\'' -> "&#39;"
    char -> Text.singleton char

matrixSendMessage ::
  Logger -> Manager -> MatrixConfig -> Text -> Text -> Maybe Text -> Maybe Value -> RenderedMessage -> IO (Either String Text)
matrixSendMessage logger manager config roomId txnId replyTo profile body = do
  request0 <-
    authorizedRequest
      config
      ( config.homeserver
          <> "/_matrix/client/v3/rooms/"
          <> roomId
          <> "/send/m.room.message/"
          <> txnId
      )
  let request =
        request0
          { method = "PUT"
          , requestBody = RequestBodyLBS (encode (matrixMessageContent replyTo profile body))
          , requestHeaders = ("Content-Type", "application/json") : request0.requestHeaders
          }
  response <- loggedHttp logger request manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixSendEdit :: Logger -> Manager -> MatrixConfig -> Text -> Text -> Text -> RenderedMessage -> IO (Either String Text)
matrixSendEdit logger manager config roomId txnId targetEventId body = do
  request0 <-
    authorizedRequest
      config
      ( config.homeserver
          <> "/_matrix/client/v3/rooms/"
          <> roomId
          <> "/send/m.room.message/"
          <> txnId
      )
  let request =
        request0
          { method = "PUT"
          , requestBody = RequestBodyLBS (encode (matrixEditContent targetEventId body))
          , requestHeaders = ("Content-Type", "application/json") : request0.requestHeaders
          }
  response <- loggedHttp logger request manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixSendMedia :: Logger -> Manager -> MatrixConfig -> Text -> Text -> Maybe Text -> Value -> IO (Either String Text)
matrixSendMedia logger manager config roomId txnId replyTo content = do
  request0 <-
    authorizedRequest
      config
      ( config.homeserver
          <> "/_matrix/client/v3/rooms/"
          <> roomId
          <> "/send/m.room.message/"
          <> txnId
      )
  let request =
        request0
          { method = "PUT"
          , requestBody = RequestBodyLBS (encode (matrixWithReply replyTo content))
          , requestHeaders = ("Content-Type", "application/json") : request0.requestHeaders
          }
  response <- loggedHttp logger request manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixUploadMedia :: Logger -> Manager -> MatrixConfig -> Text -> Text -> ByteString -> IO (Either String Text)
matrixUploadMedia logger manager config fileName mimeType bytes = go uploadEndpoints
 where
  uploadEndpoints =
    [ config.homeserver <> "/_matrix/client/v1/media/upload?filename=" <> escapePathSegment fileName
    , config.homeserver <> "/_matrix/media/v3/upload?filename=" <> escapePathSegment fileName
    ]

  go = \case
    [] -> pure (Left "matrix media upload failed")
    url : rest -> do
      request0 <- authorizedRequest config url
      let request =
            request0
              { method = "POST"
              , requestBody = RequestBodyBS bytes
              , requestHeaders = ("Content-Type", Text.encodeUtf8 mimeType) : request0.requestHeaders
              }
      response <- loggedHttp logger request manager
      case eitherDecode response.responseBody :: Either String MatrixUploadResponse of
        Right uploaded -> pure (Right uploaded.contentUri)
        Left _ -> go rest

matrixRedactMessage :: Logger -> Manager -> MatrixConfig -> Text -> Text -> Text -> IO (Either String Text)
matrixRedactMessage logger manager config roomId txnId redactedEventId = do
  request0 <-
    authorizedRequest
      config
      ( config.homeserver
          <> "/_matrix/client/v3/rooms/"
          <> roomId
          <> "/redact/"
          <> escapePathSegment redactedEventId
          <> "/"
          <> txnId
      )
  let request =
        request0
          { method = "PUT"
          , requestBody = RequestBodyLBS (encode (object []))
          , requestHeaders = ("Content-Type", "application/json") : request0.requestHeaders
          }
  response <- loggedHttp logger request manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixDownloadMedia :: Logger -> Manager -> MatrixConfig -> Text -> IO (Maybe ByteString)
matrixDownloadMedia logger manager config uri =
  case Text.stripPrefix "mxc://" uri of
    Nothing -> do
      logDebug logger ("matrix ignoring unsupported media uri " <> uri)
      pure Nothing
    Just rest -> do
      let (serverName, mediaId) = Text.breakOn "/" rest
      if Text.null serverName || Text.length mediaId < 2
        then do
          logDebug logger ("matrix ignoring malformed media uri " <> uri)
          pure Nothing
        else do
          let mediaPath = escapePathSegment serverName <> "/" <> escapePathSegment (Text.drop 1 mediaId)
          tryPaths
            [ config.homeserver <> "/_matrix/client/v1/media/download/" <> mediaPath
            , config.homeserver <> "/_matrix/media/v3/download/" <> mediaPath
            ]
 where
  tryPaths = \case
    [] -> do
      logWarn logger ("matrix media download failed for " <> uri)
      pure Nothing
    url : rest -> do
      request <- authorizedRequest config url
      result <- try (loggedHttp logger request manager) :: IO (Either HttpException (Response LByteString.ByteString))
      case result of
        Right response
          | statusCode response.responseStatus == 200 ->
              pure (Just (LByteString.toStrict response.responseBody))
        _ -> tryPaths rest

matrixMessageContent :: Maybe Text -> Maybe Value -> RenderedMessage -> Value
matrixMessageContent replyTo profile body =
  object
    ( [ "msgtype" .= ("m.text" :: Text)
      , "body" .= body.plainText
      , "format" .= ("org.matrix.custom.html" :: Text)
      , "formatted_body" .= body.htmlText
      ]
        <> maybe
          []
          (\eventId_ -> ["m.relates_to" .= object ["m.in_reply_to" .= object ["event_id" .= eventId_]]])
          replyTo
        -- MSC4144 specifies this unstable prefix until the proposal is accepted.
        <> maybe [] (\profile_ -> ["com.beeper.per_message_profile" .= profile_]) profile
    )

matrixEditContent :: Text -> RenderedMessage -> Value
matrixEditContent targetEventId body =
  object
    [ "msgtype" .= ("m.text" :: Text)
    , "body" .= ("* " <> body.plainText)
    , "format" .= ("org.matrix.custom.html" :: Text)
    , "formatted_body" .= ("* " <> body.htmlText)
    , "m.new_content"
        .= object
          [ "msgtype" .= ("m.text" :: Text)
          , "body" .= body.plainText
          , "format" .= ("org.matrix.custom.html" :: Text)
          , "formatted_body" .= body.htmlText
          ]
    , "m.relates_to" .= object ["rel_type" .= ("m.replace" :: Text), "event_id" .= targetEventId]
    ]

matrixMediaContent :: BlobType -> Text -> Text -> Int -> Text -> Value
matrixMediaContent kind fileName mimeType size mediaUrl =
  object
    [ "msgtype" .= matrixMsgType kind
    , "body" .= fileName
    , "url" .= mediaUrl
    , "info" .= object ["mimetype" .= mimeType, "size" .= size]
    ]

matrixMsgType :: BlobType -> Text
matrixMsgType = \case
  ImageBlob -> "m.image"
  VideoBlob -> "m.video"
  AudioBlob -> "m.audio"
  FileBlob -> "m.file"

matrixWithReply :: Maybe Text -> Value -> Value
matrixWithReply Nothing content = content
matrixWithReply (Just eventId_) content =
  case content of
    Object obj ->
      Object
        ( Aeson.KeyMap.insert
            "m.relates_to"
            (object ["m.in_reply_to" .= object ["event_id" .= eventId_]])
            obj
        )
    other -> other

data MatrixUploadResponse = MatrixUploadResponse
  { contentUri :: Text
  }

instance FromJSON MatrixUploadResponse where
  parseJSON = withObject "MatrixUploadResponse" \obj ->
    MatrixUploadResponse <$> obj .: "content_uri"

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

matrixRedactionKey :: Text -> PlatformKey
matrixRedactionKey matrixEventId = PlatformKey "matrix" "redaction" matrixEventId

renderEventId :: EventId -> Text
renderEventId (EventId eventId_) = eventId_

escapePathSegment :: Text -> Text
escapePathSegment = Text.decodeUtf8 . urlEncode True . Text.encodeUtf8

plainMessage :: Text -> Message
plainMessage text = Text (Plain text :| []) :| []

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
  , content :: MatrixEventContent
  }

data MatrixEventContent
  = MatrixMessage MatrixMessageContent
  | MatrixMedia MatrixMediaContent
  | MatrixRedaction Text
  | MatrixMember MatrixMembership
  | MatrixIgnored Text

data MatrixMessageContent = MatrixMessageContent
  { messageBody :: Message
  , messageReplyTo :: Maybe Text
  , messageEditOf :: Maybe Text
  }

data MatrixMediaContent = MatrixMediaContent
  { mediaKind :: BlobType
  , mediaUri :: Text
  , mediaName :: Maybe Text
  , mediaReplyTo :: Maybe Text
  }

data MatrixMembership = MatrixMembership
  { membership :: Text
  , memberUserId :: Text
  , memberDisplayName :: Maybe Text
  , memberPreviousDisplayName :: Maybe Text
  }

usableEvent :: Value -> Maybe MatrixEvent
usableEvent value = do
  obj <- asObject value
  eventType <- Aeson.KeyMap.lookup "type" obj >>= asText
  matrixEventId <- Aeson.KeyMap.lookup "event_id" obj >>= asText
  sender <- Aeson.KeyMap.lookup "sender" obj >>= asText
  let event content = Just MatrixEvent {eventId = matrixEventId, sender, content}
      ignored reason = event (MatrixIgnored reason)
      eventContent = Aeson.KeyMap.lookup "content" obj >>= asObject
  case eventType of
    "m.room.message" -> case eventContent of
      Nothing -> ignored "contentless m.room.message"
      Just content -> messageEvent event content
    "m.sticker" -> case eventContent of
      Nothing -> ignored "contentless m.sticker"
      Just content -> do
        mediaUrl <- Aeson.KeyMap.lookup "url" content >>= asText
        event
          ( MatrixMedia
              MatrixMediaContent
                { mediaKind = ImageBlob
                , mediaUri = mediaUrl
                , mediaName = Aeson.KeyMap.lookup "body" content >>= asText
                , mediaReplyTo = Nothing
                }
          )
    "m.reaction" -> ignored "m.reaction"
    "m.room.encrypted" -> ignored "m.room.encrypted"
    "m.room.redaction" -> do
      let redactedEvent =
            (Aeson.KeyMap.lookup "redacts" obj >>= asText)
              <|> (Aeson.KeyMap.lookup "content" obj >>= asObject >>= Aeson.KeyMap.lookup "redacts" >>= asText)
      MatrixEvent matrixEventId sender . MatrixRedaction <$> redactedEvent
    "m.room.member" -> do
      content <- eventContent
      membership <- Aeson.KeyMap.lookup "membership" content >>= asText
      memberUserId <- Aeson.KeyMap.lookup "state_key" obj >>= asText
      let previousContent =
            Aeson.KeyMap.lookup "unsigned" obj
              >>= asObject
              >>= Aeson.KeyMap.lookup "prev_content"
              >>= asObject
          previousDisplayName = previousContent >>= Aeson.KeyMap.lookup "displayname" >>= asText
      event
        ( MatrixMember
            MatrixMembership
              { membership
              , memberUserId
              , memberDisplayName = Aeson.KeyMap.lookup "displayname" content >>= asText
              , memberPreviousDisplayName = previousDisplayName
              }
        )
    _ -> ignored eventType

messageEvent :: (MatrixEventContent -> Maybe MatrixEvent) -> Object -> Maybe MatrixEvent
messageEvent event content = do
  let relatesTo = Aeson.KeyMap.lookup "m.relates_to" content >>= asObject
      replyTo =
        relatesTo
          >>= Aeson.KeyMap.lookup "m.in_reply_to"
          >>= asObject
          >>= Aeson.KeyMap.lookup "event_id"
          >>= asText
      editOf = do
        relates <- relatesTo
        relType <- Aeson.KeyMap.lookup "rel_type" relates >>= asText
        guardMaybe (relType == "m.replace")
        Aeson.KeyMap.lookup "event_id" relates >>= asText
      replacement = Aeson.KeyMap.lookup "m.new_content" content >>= asObject
      effective = fromMaybe content replacement
  msgtype <- Aeson.KeyMap.lookup "msgtype" effective >>= asText
  body <- Aeson.KeyMap.lookup "body" effective >>= asText
  let delivered messageBody =
        event
          ( MatrixMessage
              MatrixMessageContent
                { messageBody
                , messageReplyTo = replyTo
                , messageEditOf = editOf
                }
          )
      media kind = case Aeson.KeyMap.lookup "url" effective >>= asText of
        Nothing -> event (MatrixIgnored ("contentless " <> msgtype))
        Just mediaUrl ->
          event
            ( MatrixMedia
                MatrixMediaContent
                  { mediaKind = kind
                  , mediaUri = mediaUrl
                  , mediaName = Just body
                  , mediaReplyTo = replyTo
                  }
            )
  case msgtype of
    "m.text" -> delivered (matrixMessageBody effective body)
    "m.notice" -> delivered (matrixMessageBody effective body)
    "m.emote" -> delivered (matrixEmoteBody effective body)
    "m.location" -> delivered (matrixLocationBody effective body)
    "m.image" -> media ImageBlob
    "m.video" -> media VideoBlob
    "m.audio" -> media AudioBlob
    "m.file" -> media FileBlob
    _ -> event (MatrixIgnored ("msgtype " <> msgtype))

matrixEmoteBody :: Object -> Text -> Message
matrixEmoteBody content fallback =
  case matrixMessageBody content fallback of
    Text inline :| [] -> Emote inline :| []
    body -> body

matrixLocationBody :: Object -> Text -> Message
matrixLocationBody content fallback =
  let label = if Text.null fallback then "location" else fallback
      inline = case Aeson.KeyMap.lookup "geo_uri" content >>= asText >>= geoMapLink of
        Just url -> Link (Plain label :| []) url
        Nothing -> Plain label
   in Text (inline :| []) :| []

geoMapLink :: Text -> Maybe Text
geoMapLink uri = do
  rest <- Text.stripPrefix "geo:" uri
  let (coordinates, _) = Text.breakOn ";" rest
      (latitude, longitude) = Text.breakOn "," coordinates
  guardMaybe (not (Text.null latitude) && Text.length longitude > 1)
  pure ("https://www.openstreetmap.org/?mlat=" <> latitude <> "&mlon=" <> Text.drop 1 longitude)

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

matrixMessageBody :: Object -> Text -> Message
matrixMessageBody content fallback =
  case (Aeson.KeyMap.lookup "format" content >>= asText, Aeson.KeyMap.lookup "formatted_body" content >>= asText) of
    (Just "org.matrix.custom.html", Just html) -> matrixHtmlMessage html
    _ -> plainMessage fallback

matrixHtmlMessage :: Text -> Message
matrixHtmlMessage html =
  case parseMessageParts (parseTags html) of
    part : parts -> part :| parts
    [] -> plainMessage (innerText (parseTags html))

parseMessageParts :: [Tag Text] -> [MessagePart]
parseMessageParts = go []
 where
  go acc [] = reverse acc
  go acc tags =
    case break blockStart tags of
      (inlineTags, []) ->
        reverse (appendInline acc inlineTags)
      (inlineTags, start : rest) ->
        let accWithInline = appendInline acc inlineTags
            (part, afterBlock) = parseBlock start rest
         in go (part : accWithInline) afterBlock

appendInline :: [MessagePart] -> [Tag Text] -> [MessagePart]
appendInline parts tags =
  case matrixInlineFromTags tags of
    Just inline -> Text inline : parts
    Nothing -> parts

blockStart :: Tag Text -> Bool
blockStart tag =
  any (tagOpenNameLit tag) ["blockquote", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6"]

parseBlock :: Tag Text -> [Tag Text] -> (MessagePart, [Tag Text])
parseBlock start rest
  | tagOpenNameLit start "blockquote" =
      let (inside, after) = takeUntilClose "blockquote" rest
       in (BlockQuote (messageFromTags inside), after)
  | tagOpenNameLit start "ul" || tagOpenNameLit start "ol" =
      let closeTag = if tagOpenNameLit start "ul" then "ul" else "ol"
          (inside, after) = takeUntilClose closeTag rest
          items = parseListItems inside
       in case nonEmpty items of
            Just nonEmptyItems -> (List nonEmptyItems, after)
            Nothing -> (Text (Plain "" :| []), after)
  | otherwise =
      case headingLevel start of
        Just level ->
          let tagName = "h" <> Text.pack (show level)
              (inside, after) = takeUntilClose tagName rest
           in (Heading level (matrixInlineFromTagsOrPlain inside), after)
        Nothing -> (Text (Plain (innerText [start]) :| []), rest)

messageFromTags :: [Tag Text] -> Message
messageFromTags tags =
  case parseMessageParts tags of
    part : parts -> part :| parts
    [] -> Text (Plain (innerText tags) :| []) :| []

parseListItems :: [Tag Text] -> [Message]
parseListItems [] = []
parseListItems (tag : rest)
  | tagOpenNameLit tag "li" =
      let (inside, after) = takeUntilClose "li" rest
       in messageFromTags inside : parseListItems after
  | otherwise = parseListItems rest

headingLevel :: Tag Text -> Maybe Int
headingLevel tag =
  asum [level <$ guardMaybe (tagOpenNameLit tag ("h" <> Text.pack (show level))) | level <- [1 .. 6]]

takeUntilClose :: Text -> [Tag Text] -> ([Tag Text], [Tag Text])
takeUntilClose name = go 0 []
 where
  go _ acc [] = (reverse acc, [])
  go depth acc (tag : rest)
    | tagOpenNameLit tag name = go (depth + 1) (tag : acc) rest
    | tagCloseNameLit tag name && depth == 0 = (reverse acc, rest)
    | tagCloseNameLit tag name = go (depth - 1) (tag : acc) rest
    | otherwise = go depth (tag : acc) rest

matrixHtmlInline :: Text -> InlineText
matrixHtmlInline = matrixInlineFromTagsOrPlain . parseTags

matrixInlineFromTagsOrPlain :: [Tag Text] -> InlineText
matrixInlineFromTagsOrPlain tags =
  case matrixInlineFromTags tags of
    Just inline -> inline
    Nothing -> Plain (innerText tags) :| []

matrixInlineFromTags :: [Tag Text] -> Maybe InlineText
matrixInlineFromTags tags =
  nonEmpty (parseInlineTags tags)

parseInlineTags :: [Tag Text] -> [InlineTextPart]
parseInlineTags = \case
  [] -> []
  TagText text : rest -> [Plain text | not (Text.null text)] <> parseInlineTags rest
  tag : rest
    | tagOpenNameLit tag "strong" || tagOpenNameLit tag "b" ->
        wrapped Bold tag rest
    | tagOpenNameLit tag "em" || tagOpenNameLit tag "i" ->
        wrapped Italic tag rest
    | tagOpenNameLit tag "code" ->
        wrapped InlineQuote tag rest
    | tagOpenNameLit tag "a" ->
        let (inside, after) = takeUntilClose "a" rest
            url = maybe (innerText inside) id (lookup "href" (tagAttributes tag))
         in Link (matrixInlineFromTagsOrPlain inside) url : parseInlineTags after
    | tagOpenNameLit tag "br" -> Plain "\n" : parseInlineTags rest
    | otherwise -> parseInlineTags rest
 where
  wrapped constructor tag rest =
    let name = tagNameText tag
        (inside, after) = takeUntilClose name rest
     in constructor (matrixInlineFromTagsOrPlain inside) : parseInlineTags after

tagOpenNameLit :: Tag Text -> Text -> Bool
tagOpenNameLit tag name =
  case tag of
    TagOpen tagName _ -> Text.toLower tagName == name
    _ -> False

tagCloseNameLit :: Tag Text -> Text -> Bool
tagCloseNameLit tag name =
  case tag of
    TagClose tagName -> Text.toLower tagName == name
    _ -> False

tagNameText :: Tag Text -> Text
tagNameText = \case
  TagOpen name _ -> Text.toLower name
  TagClose name -> Text.toLower name
  _ -> ""

tagAttributes :: Tag Text -> [(Text, Text)]
tagAttributes = \case
  TagOpen _ attrs -> attrs
  _ -> []

data MatrixSendResponse = MatrixSendResponse
  { eventId :: Text
  }

instance FromJSON MatrixSendResponse where
  parseJSON = withObject "MatrixSendResponse" \obj ->
    MatrixSendResponse <$> obj .: "event_id"
