module Multiverse.Matrix
  ( matrixBridge
  )
where

import Control.Concurrent (threadDelay)
import Control.Applicative (asum, (<|>))
import Control.Exception (try)
import Control.Monad (forM_, forever, unless, when)
import Data.Aeson
import Data.Aeson.Types (Parser)
import Data.Aeson.Key qualified as Aeson.Key
import Data.Aeson.KeyMap qualified as Aeson.KeyMap
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LByteString
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.HTTP.Types.Status (statusCode)
import Network.HTTP.Types.URI (urlEncode)
import Text.HTML.TagSoup
import Multiverse.Bridge
import Multiverse.Config (InitialRoomMapping (..), MatrixBridgeConfig (..))
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
  , config :: MatrixBridgeConfig
  }

data Observer timeline = Observer
  { bridge :: Context timeline
  , ownUserId :: Text
  }

matrixBridge :: Timeline timeline => BridgeRuntime timeline -> MatrixBridgeConfig -> IO (Bridge, MatrixBridgeConfig)
matrixBridge runtime initialConfig = do
  mappingStore <- openMappingStore initialConfig.mappingDb
  manager <- newManager tlsManagerSettings
  let provisionalContext =
        Context
          { timeline = runtime.timeline
          , mappingStore
          , logger = runtime.logger
          , manager
          , config = initialConfig
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
  logInfo runtime.logger ("matrix bridge started with mapping db " <> Text.pack config.mappingDb)
  pure
    ( Bridge
        { bridgeName = "matrix"
        , observe = observeMatrix context
        , reflect = reflectMatrix context
        }
    , config
    )

observeMatrix :: Timeline timeline => Context timeline -> IO ()
observeMatrix context = do
  whoami <- matrixWhoami context
  ownUserId <-
    case whoami of
      Right value -> do
        logInfo context.logger ("matrix bridge user id " <> value.userId)
        pure value.userId
      Left err -> ioError (userError ("matrix whoami failed: " <> err))
  let observer = Observer {bridge = context, ownUserId}
  forever do
    since <- context.mappingStore.lookupState "matrix.next_batch"
    response <- matrixSync context since
    case response of
      Left err -> do
        logError context.logger ("matrix observe error: " <> Text.pack err)
        threadDelay 5000000
      Right sync -> do
        mapM_ (observeRoomEvents observer) sync.rooms
        context.mappingStore.setState "matrix.next_batch" sync.nextBatch

reflectMatrix :: Timeline timeline => Context timeline -> IO ()
reflectMatrix context =
  forever do
    afterEventId <- fmap (fmap EventId) (context.mappingStore.lookupState "matrix.reflect_event")
    storedEvents <- getEventsAfter context.timeline afterEventId
    mapM_ (reflectEvent context) storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> context.mappingStore.setState "matrix.reflect_event" (renderEventId ((last storedEvents).storedId))

observeRoomEvents :: Timeline timeline => Observer timeline -> MatrixRoomEvents -> IO ()
observeRoomEvents observer roomEvents = do
  room <- configuredRoom observer.bridge roomEvents.roomId
  case room of
    Nothing -> logDebug observer.bridge.logger ("matrix ignoring unconfigured room " <> roomEvents.roomId)
    Just roomId -> mapM_ (observeEvent observer roomEvents.roomId roomId) roomEvents.events

observeEvent :: Timeline timeline => Observer timeline -> Text -> RoomId -> MatrixEvent -> IO ()
observeEvent observer platformRoom room event
  | event.sender == observer.ownUserId, not (isMemberEvent event.content) =
      logDebug observer.bridge.logger ("matrix ignoring own event " <> event.eventId)
  | otherwise =
      case event.content of
        MatrixMessage messageContent -> observeMatrixMessage observer event platformRoom room messageContent
        MatrixMedia mediaContent -> observeMatrixMedia observer event platformRoom room mediaContent
        MatrixRedaction redactedEvent -> do
          target <- observer.bridge.mappingStore.lookupTimelineId (matrixEventKey platformRoom redactedEvent)
          case target of
            Nothing -> logDebug observer.bridge.logger ("matrix ignoring redaction for unknown event " <> redactedEvent)
            Just targetEventId -> do
              let timelineEvent =
                    Event
                      { platformKey = matrixRedactionKey platformRoom event.eventId
                      , content = RetractMessage (MessageId targetEventId)
                      }
              submitted <- submitMapped observer.bridge.timeline observer.bridge.mappingStore timelineEvent
              case submitted of
                Right _ -> pure ()
                Left (MissingReferences missing)
                  | targetEventId `elem` missing ->
                      logDebug observer.bridge.logger ("matrix ignoring redaction for missing timeline event " <> renderEventId targetEventId)
                Left err -> logError observer.bridge.logger ("matrix retraction submit error: " <> Text.pack (show err))
        MatrixMember member -> observeMatrixMember observer room event member
        MatrixIgnored reason
          | reason == "m.room.encrypted" ->
              logWarn observer.bridge.logger ("matrix cannot decrypt event " <> event.eventId <> ": encrypted rooms are not supported")
          | otherwise -> logDebug observer.bridge.logger ("matrix ignoring " <> reason <> " event " <> event.eventId)

observeMatrixMessage :: Timeline timeline => Observer timeline -> MatrixEvent -> Text -> RoomId -> MatrixMessageContent -> IO ()
observeMatrixMessage observer event platformRoom room messageContent = do
  let context = observer.bridge
  userId <- ensureUser observer event.sender
  ensureRoomMembership context.logger context.timeline context.mappingStore "matrix" room userId
  case messageContent.messageEditOf of
    Just targetEventId -> do
      target <- context.mappingStore.lookupTimelineId (matrixEventKey platformRoom targetEventId)
      case target of
        Nothing -> logDebug context.logger ("matrix ignoring edit for unknown event " <> targetEventId)
        Just targetMessage -> do
          let timelineEvent =
                Event
                  { platformKey = PlatformKey "matrix" "edit" (platformRoom <> "/" <> event.eventId)
                  , content = EditMessage (MessageId targetMessage) messageContent.messageBody
                  }
          submitted <- submitMapped context.timeline context.mappingStore timelineEvent
          case submitted of
            Right _ -> pure ()
            Left err -> logError context.logger ("matrix edit submit error: " <> Text.pack (show err))
    Nothing -> do
      replyTo <- matrixReplyTo context platformRoom messageContent.messageReplyTo
      let timelineEvent =
            Event
              { platformKey = matrixEventKey platformRoom event.eventId
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
      submitted <- submitMessageMapped context.timeline context.mappingStore timelineEvent
      case submitted of
        Right _ -> pure ()
        Left err -> logError context.logger ("matrix submit error: " <> Text.pack (show err))

observeMatrixMedia :: Timeline timeline => Observer timeline -> MatrixEvent -> Text -> RoomId -> MatrixMediaContent -> IO ()
observeMatrixMedia observer event platformRoom room mediaContent = do
  let context = observer.bridge
  userId <- ensureUser observer event.sender
  ensureRoomMembership context.logger context.timeline context.mappingStore "matrix" room userId
  let blobKey = PlatformKey "matrix" "blob" mediaContent.mediaUri
  existing <- context.mappingStore.lookupTimelineId blobKey
  blobId <- case existing of
    Just blobEvent -> pure (Just (BlobId blobEvent))
    Nothing -> do
      downloaded <- matrixDownloadMedia context mediaContent.mediaUri
      case downloaded of
        Nothing -> pure Nothing
        Just bytes -> do
          submitted <- submitBlob context.timeline context.mappingStore blobKey bytes
          case submitted of
            Right blob -> pure (Just blob)
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
      replyTo <- matrixReplyTo context platformRoom mediaContent.mediaReplyTo
      let timelineEvent =
            Event
              { platformKey = matrixEventKey platformRoom event.eventId
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
      submitted <- submitMessageMapped context.timeline context.mappingStore timelineEvent
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

observeMatrixMember :: Timeline timeline => Observer timeline -> RoomId -> MatrixEvent -> MatrixMembership -> IO ()
observeMatrixMember observer room event member
  | member.memberUserId == observer.ownUserId =
      logDebug observer.bridge.logger ("matrix ignoring own membership " <> member.memberUserId)
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
  context = observer.bridge
  withUser action = do
    user <- ensureUser observer member.memberUserId
    action user

  eventKey kind = PlatformKey "matrix" kind (event.eventId <> "/" <> member.memberUserId)

  submitEvent platformKey content = do
    result <- submitMapped context.timeline context.mappingStore Event {platformKey, content}
    case result of
      Left err -> logError context.logger ("matrix membership submit error: " <> Text.pack (show err))
      Right _ -> pure ()

  submitMembership kind user =
    submitEvent (eventKey kind) (if kind == "join" then JoinRoom user room else LeaveRoom user room)

  submitNick user nick = submitEvent (eventKey "nick") (ChangeRoomNick user room nick)

matrixReplyTo :: Context timeline -> Text -> Maybe Text -> IO (Maybe MessageId)
matrixReplyTo context platformRoom replyToEvent =
  case replyToEvent of
    Nothing -> pure Nothing
    Just repliedEvent -> do
      mapped <- context.mappingStore.lookupTimelineId (matrixEventKey platformRoom repliedEvent)
      pure (MessageId <$> mapped)

ensureUser :: Timeline timeline => Observer timeline -> Text -> IO UserId
ensureUser observer matrixUserId = do
  let context = observer.bridge
  let platformKey = matrixUserKey matrixUserId
  existing <- context.mappingStore.lookupTimelineId platformKey
  case existing of
    Just eventId -> pure (UserId eventId)
    Nothing -> do
      displayName <- matrixUserDisplayName context matrixUserId
      let event = Event {platformKey, content = CreateUser UserInfo {name = maybe matrixUserId id displayName, avatar = Nothing}}
      mapped <- submitMapped context.timeline context.mappingStore event
      case mapped of
        Right eventId -> pure (UserId eventId)
        Left err -> fail ("could not create matrix user: " <> show err)

configuredRoom :: Context timeline -> Text -> IO (Maybe RoomId)
configuredRoom context matrixRoomId = do
  let platformKey = matrixRoomKey matrixRoomId
  if enabledRoom context.config.initialRooms platformKey
    then fmap RoomId <$> context.mappingStore.lookupTimelineId platformKey
    else pure Nothing

reflectEvent :: Timeline timeline => Context timeline -> StoredEvent -> IO ()
reflectEvent context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
          let matrixRooms = filter (enabledRoom context.config.initialRooms) roomKeys
          (profile, body) <- renderMatrixRelayMessage context info {body = textBody info.body}
          mapM_ (sendToRoom info profile body) matrixRooms
    EditMessage message body
      | stored.storedEvent.platformKey.platform /= "matrix" -> do
          original <- getEvent context.timeline (messageEventId message)
          case fmap (.storedEvent.content) original of
            Just (SendMessage info) -> do
              unless (messageBlobs info.body == messageBlobs body) do
                logWarn context.logger "matrix cannot reflect changes to message attachments"
              text <- renderRelayedMessage context.timeline info {body = textBody body}
              targets <- matrixEditTargets context message
              alreadyMapped <- hasPlatformMapping context.mappingStore "matrix" stored.storedId
              unless alreadyMapped do
                forM_ targets \(roomId, matrixEventId) -> do
                  let txnId = matrixTransactionId stored "edit" roomId
                  result <- matrixSendEdit context roomId txnId matrixEventId text
                  case result of
                    Left err -> logError context.logger ("matrix edit reflect error: " <> Text.pack err)
                    Right editEventId -> context.mappingStore.insertMapping (matrixEventKey roomId editEventId) stored.storedId
            _ -> pure ()
    RetractMessage message
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          alreadyMapped <- hasPlatformMapping context.mappingStore "matrix" stored.storedId
          unless alreadyMapped do
            targets <- matrixRetractTargets context message
            mapM_ redactInRoom (filter (enabledRoom context.config.initialRooms . matrixRoomKey . fst) targets)
    ChangeRoomNick user room nick
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin nick change " <> renderEventId stored.storedId)
      | otherwise -> do
          body <- renderNickChange context.timeline user nick
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
          forM_ (filter (enabledRoom context.config.initialRooms) roomKeys) \roomKey -> do
            alreadySent <- matrixMessageMappedToRoom context stored.storedId roomKey.key
            unless alreadySent do
              let txnId = matrixTransactionId stored "nick" roomKey.key
              result <- matrixSendMessage context roomKey.key txnId Nothing Nothing body
              case result of
                Left err -> logError context.logger ("matrix nick reflect error: " <> Text.pack err)
                Right matrixEventId ->
                  context.mappingStore.insertMapping (matrixEventKey roomKey.key matrixEventId) stored.storedId
    _ -> pure ()
 where
  sendToRoom info profile body roomKey = do
    alreadySent <- matrixMessageMappedToRoom context stored.storedId roomKey.key
    unless alreadySent do
      replyTo <- matrixReplyEventId context roomKey.key info.replyTo
      let roomId = roomKey.key
          blobs = messageBlobs info.body
          sendPart action = do
            result <- action
            case result of
              Left err -> logError context.logger ("matrix reflect error: " <> Text.pack err)
              Right matrixEventId ->
                context.mappingStore.insertMapping (matrixEventKey roomId matrixEventId) stored.storedId
          sendText = sendPart (matrixSendMessage context roomId (matrixTransactionId stored "text" roomId) replyTo (Just profile) body)
          sendBlob index (kind, blobId) = do
            blob <- getEvent context.timeline (blobEventId blobId)
            case fmap (.storedEvent.content) blob of
              Just (CreateBlob bytes) -> do
                let name = "file-" <> Text.take 12 (renderEventId (blobEventId blobId)) <> sniffExtension bytes
                    mimeType = sniffMimeType bytes
                    stateKey = "matrix.blob." <> renderEventId (blobEventId blobId)
                cached <- context.mappingStore.lookupState stateKey
                uploaded <- case cached of
                  Just mediaUrl -> pure (Right mediaUrl)
                  Nothing -> matrixUploadMedia context name mimeType bytes
                case uploaded of
                  Left err -> logError context.logger ("matrix media upload error: " <> Text.pack err)
                  Right mediaUrl -> do
                    context.mappingStore.setState stateKey mediaUrl
                    let blobReplyTo = if index == 1 && not (hasTextContent info.body) then replyTo else Nothing
                        content = matrixMediaContent kind name mimeType (ByteString.length bytes) mediaUrl
                        partTxnId = matrixTransactionId stored ("part" <> Text.pack (show index)) roomId
                    sendPart (matrixSendMedia context roomId partTxnId blobReplyTo content)
              _ -> logError context.logger "matrix media blob is missing"
      when (hasTextContent info.body) sendText
      forM_ (zip [1 :: Int ..] blobs) (uncurry sendBlob)
  redactInRoom (roomId, eventId_) = do
    let txnId = matrixTransactionId stored "redact" roomId
    result <- matrixRedactMessage context roomId txnId eventId_
    case result of
      Left err -> do
        logError context.logger ("matrix retract reflect error: " <> Text.pack err)
        let fallbackTxnId = matrixTransactionId stored "redact-failure" roomId
        fallback <- matrixSendMessage context roomId fallbackTxnId Nothing Nothing (retractionFailureMessage err)
        case fallback of
          Left fallbackErr -> logError context.logger ("matrix retract failure notice error: " <> Text.pack fallbackErr)
          Right matrixEventId -> context.mappingStore.insertMapping (matrixEventKey roomId matrixEventId) stored.storedId
      Right redactionEventId -> do
        let mappingKey =
              maybe
                (PlatformKey "matrix" "redaction" (roomId <> "/" <> renderEventId stored.storedId))
                (matrixRedactionKey roomId)
                redactionEventId
        context.mappingStore.insertMapping mappingKey stored.storedId

-- | Transaction ids are idempotency keys scoped to the access token, so they
-- must be unique per room and per part.
matrixTransactionId :: StoredEvent -> Text -> Text -> Text
matrixTransactionId stored suffix roomId =
  escapePathSegment (renderEventId stored.storedId <> "-" <> suffix <> "-" <> roomId)

retractionFailureMessage :: String -> RenderedMessage
retractionFailureMessage err =
  let text = "Failed to retract a bridged message: " <> Text.pack err
   in RenderedMessage text text

matrixEditTargets :: Context timeline -> MessageId -> IO [(Text, Text)]
matrixEditTargets context message = do
  targets <- matrixMessageTargets context (messageEventId message)
  pure (filter (enabledRoom context.config.initialRooms . matrixRoomKey . fst) targets)

matrixRetractTargets :: Context timeline -> MessageId -> IO [(Text, Text)]
matrixRetractTargets context message = matrixMessageTargets context (messageEventId message)

-- | Every Matrix event this timeline event is reflected as, as @(room, event)@.
-- Observed and reflected messages are both recorded as plain mapping rows, so a
-- single lookup covers both.
matrixMessageTargets :: Context timeline -> EventId -> IO [(Text, Text)]
matrixMessageTargets context eventId_ = do
  keys <- context.mappingStore.lookupPlatformKeys eventId_
  pure (mapMaybe matrixMessageTarget keys)

matrixMessageTarget :: PlatformKey -> Maybe (Text, Text)
matrixMessageTarget platformKey
  | platformKey.platform /= "matrix" = Nothing
  | platformKey.entityType /= "message" = Nothing
  | otherwise =
      case Text.splitOn "/" platformKey.key of
        [roomId, matrixEventId] -> Just (roomId, matrixEventId)
        _ -> Nothing

matrixMessageMappedToRoom :: Context timeline -> EventId -> Text -> IO Bool
matrixMessageMappedToRoom context eventId_ roomId = do
  keys <- context.mappingStore.lookupPlatformKeys eventId_
  pure (any ((== Just roomId) . fmap fst . matrixMessageTarget) keys)

matrixReplyEventId :: Context timeline -> Text -> Maybe MessageId -> IO (Maybe Text)
matrixReplyEventId _ _ Nothing = pure Nothing
matrixReplyEventId context roomId (Just message) = do
  keys <- context.mappingStore.lookupPlatformKeys (messageEventId message)
  pure case [matrixEventId | key <- keys, Just (targetRoom, matrixEventId) <- [matrixMessageTarget key], targetRoom == roomId] of
    eventId_ : _ -> Just eventId_
    [] -> Nothing

matrixSync :: Context timeline -> Maybe Text -> IO (Either String MatrixSync)
matrixSync context since = do
  request0 <- authorizedRequest context (context.config.homeserver <> "/_matrix/client/v3/sync")
  let query =
        [ ("timeout", Just (Text.encodeUtf8 (Text.pack (show context.config.syncTimeoutMs))))
        ]
          <> maybe [] (\token -> [("since", Just (Text.encodeUtf8 token))]) since
      request = setQueryString query request0
  response <- loggedHttp context.logger request context.manager
  pure (decodeMatrix response)

matrixWhoami :: Context timeline -> IO (Either String MatrixWhoami)
matrixWhoami context = do
  request <- authorizedRequest context (context.config.homeserver <> "/_matrix/client/v3/account/whoami")
  response <- loggedHttp context.logger request context.manager
  pure (decodeMatrix response)

matrixUserDisplayName :: Context timeline -> Text -> IO (Maybe Text)
matrixUserDisplayName context matrixUserId = do
  result <- matrixGetDisplayName context matrixUserId
  case result of
    Right value -> pure value.displayName
    Left err -> do
      logDebug context.logger ("matrix display name lookup failed for " <> matrixUserId <> ": " <> Text.pack err)
      pure Nothing

matrixGetDisplayName :: Context timeline -> Text -> IO (Either String MatrixDisplayName)
matrixGetDisplayName context matrixUserId = do
  request <- authorizedRequest context (context.config.homeserver <> "/_matrix/client/v3/profile/" <> escapePathSegment matrixUserId <> "/displayname")
  response <- loggedHttp context.logger request context.manager
  pure (decodeMatrix response)

renderMatrixRelayMessage :: Timeline timeline => Context timeline -> SendMessageInfo -> IO (Value, RenderedMessage)
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
  Context timeline -> Text -> Text -> Maybe Text -> Maybe Value -> RenderedMessage -> IO (Either String Text)
matrixSendMessage context roomId txnId replyTo profile body = do
  request0 <-
    authorizedRequest
      context
      ( context.config.homeserver
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
  response <- loggedHttp context.logger request context.manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixSendEdit :: Context timeline -> Text -> Text -> Text -> RenderedMessage -> IO (Either String Text)
matrixSendEdit context roomId txnId targetEventId body = do
  request0 <-
    authorizedRequest
      context
      ( context.config.homeserver
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
  response <- loggedHttp context.logger request context.manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixSendMedia :: Context timeline -> Text -> Text -> Maybe Text -> Value -> IO (Either String Text)
matrixSendMedia context roomId txnId replyTo content = do
  request0 <-
    authorizedRequest
      context
      ( context.config.homeserver
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
  response <- loggedHttp context.logger request context.manager
  pure case eitherDecode response.responseBody :: Either String MatrixSendResponse of
    Left err -> Left err
    Right sent -> Right sent.eventId

matrixUploadMedia :: Context timeline -> Text -> Text -> ByteString -> IO (Either String Text)
matrixUploadMedia context fileName mimeType bytes = go uploadEndpoints
 where
  uploadEndpoints =
    [ context.config.homeserver <> "/_matrix/client/v1/media/upload?filename=" <> escapePathSegment fileName
    , context.config.homeserver <> "/_matrix/media/v3/upload?filename=" <> escapePathSegment fileName
    ]

  go = \case
    [] -> pure (Left "matrix media upload failed")
    url : rest -> do
      request0 <- authorizedRequest context url
      let request =
            request0
              { method = "POST"
              , requestBody = RequestBodyBS bytes
              , requestHeaders = ("Content-Type", Text.encodeUtf8 mimeType) : request0.requestHeaders
              }
      response <- loggedHttp context.logger request context.manager
      case eitherDecode response.responseBody :: Either String MatrixUploadResponse of
        Right uploaded -> pure (Right uploaded.contentUri)
        Left _ -> go rest

matrixRedactMessage :: Context timeline -> Text -> Text -> Text -> IO (Either String (Maybe Text))
matrixRedactMessage context roomId txnId redactedEventId = do
  request0 <-
    authorizedRequest
      context
      ( context.config.homeserver
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
  response <- loggedHttp context.logger request context.manager
  let status = statusCode response.responseStatus
  if status < 200 || status >= 300
    then pure (Left ("matrix redact failed with status " <> show status))
    else pure case eitherDecode response.responseBody :: Either String MatrixRedactionResponse of
      Right sent -> Right sent.eventId
      Left _ -> Right Nothing

matrixDownloadMedia :: Context timeline -> Text -> IO (Maybe ByteString)
matrixDownloadMedia context uri =
  case Text.stripPrefix "mxc://" uri of
    Nothing -> do
      logDebug context.logger ("matrix ignoring unsupported media uri " <> uri)
      pure Nothing
    Just rest -> do
      let (serverName, mediaId) = Text.breakOn "/" rest
      if Text.null serverName || Text.length mediaId < 2
        then do
          logDebug context.logger ("matrix ignoring malformed media uri " <> uri)
          pure Nothing
        else do
          let mediaPath = escapePathSegment serverName <> "/" <> escapePathSegment (Text.drop 1 mediaId)
          tryPaths
            [ context.config.homeserver <> "/_matrix/client/v1/media/download/" <> mediaPath
            , context.config.homeserver <> "/_matrix/media/v3/download/" <> mediaPath
            ]
 where
  tryPaths = \case
    [] -> do
      logWarn context.logger ("matrix media download failed for " <> uri)
      pure Nothing
    url : rest -> do
      request <- authorizedRequest context url
      result <- try (loggedHttp context.logger request context.manager) :: IO (Either HttpException (Response LByteString.ByteString))
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

fetchInitialRoomInfo :: Context timeline -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo context mapping = do
  name <- matrixRoomName context mapping.platformKey.key
  topic <- matrixRoomTopic context mapping.platformKey.key
  pure
    RoomInfo
      { name = maybe mapping.platformKey.key id name
      , description = maybe "" id topic
      , avatar = Nothing
      }

matrixRoomName :: Context timeline -> Text -> IO (Maybe Text)
matrixRoomName context roomId = do
  result <- matrixGetState context roomId "m.room.name" :: IO (Either String MatrixRoomNameState)
  pure case result of
    Right value -> value.name
    Left _ -> Nothing

matrixRoomTopic :: Context timeline -> Text -> IO (Maybe Text)
matrixRoomTopic context roomId = do
  result <- matrixGetState context roomId "m.room.topic" :: IO (Either String MatrixRoomTopicState)
  pure case result of
    Right value -> value.topic
    Left _ -> Nothing

matrixGetState :: FromJSON a => Context timeline -> Text -> Text -> IO (Either String a)
matrixGetState context roomId eventType = do
  request <- authorizedRequest context (context.config.homeserver <> "/_matrix/client/v3/rooms/" <> roomId <> "/state/" <> eventType)
  response <- loggedHttp context.logger request context.manager
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

authorizedRequest :: Context timeline -> Text -> IO Request
authorizedRequest context url = do
  request <- parseRequest (Text.unpack url)
  pure
    request
      { requestHeaders = ("Authorization", Text.encodeUtf8 ("Bearer " <> context.config.accessToken)) : request.requestHeaders
      }

decodeMatrix :: FromJSON a => Response LByteString.ByteString -> Either String a
decodeMatrix response = eitherDecode response.responseBody

matrixUserKey :: Text -> PlatformKey
matrixUserKey userId = PlatformKey "matrix" "user" userId

matrixRoomKey :: Text -> PlatformKey
matrixRoomKey roomId = PlatformKey "matrix" "room" roomId

-- | Matrix event ids do not name their room and the client API cannot look it
-- up, so the room is recorded in the platform key. Neither room ids nor event
-- ids contain a slash, which keeps the key parseable.
matrixEventKey :: Text -> Text -> PlatformKey
matrixEventKey roomId matrixEventId = PlatformKey "matrix" "message" (roomId <> "/" <> matrixEventId)

matrixRedactionKey :: Text -> Text -> PlatformKey
matrixRedactionKey roomId matrixEventId = PlatformKey "matrix" "redaction" (roomId <> "/" <> matrixEventId)

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
takeUntilClose name = go (0 :: Int) []
 where
  go _ acc [] = (reverse acc, [])
  go depth acc (tag : rest)
    | tagOpenNameLit tag name = go (depth + 1) (tag : acc) rest
    | tagCloseNameLit tag name && depth == 0 = (reverse acc, rest)
    | tagCloseNameLit tag name = go (depth - 1) (tag : acc) rest
    | otherwise = go depth (tag : acc) rest

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

data MatrixRedactionResponse = MatrixRedactionResponse
  { eventId :: Maybe Text
  }

instance FromJSON MatrixRedactionResponse where
  parseJSON = withObject "MatrixRedactionResponse" \obj ->
    MatrixRedactionResponse <$> obj .:? "event_id"
