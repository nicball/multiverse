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
import Data.Maybe (fromMaybe, isJust, isNothing, listToMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
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
  reflectForever context.timeline context.mappingStore "matrix.reflect_event" (reflectEvent context)

observeRoomEvents :: Timeline timeline => Observer timeline -> MatrixRoomEvents -> IO ()
observeRoomEvents observer roomEvents = do
  room <-
    lookupConfiguredRoom
      observer.bridge.mappingStore
      observer.bridge.config.initialRooms
      (matrixRoomKey roomEvents.roomId)
  case room of
    Nothing -> logDebug observer.bridge.logger ("matrix ignoring unconfigured room " <> roomEvents.roomId)
    Just roomId -> mapM_ (observeEvent observer roomEvents.roomId roomId) roomEvents.events

observeEvent :: Timeline timeline => Observer timeline -> Text -> RoomId -> MatrixEvent -> IO ()
observeEvent observer platformRoom room event
  | event.sender == observer.ownUserId, not (isMember event.content) =
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
              root <- resolveMessageId observer.bridge.timeline targetEventId
              case root of
                Nothing -> logDebug observer.bridge.logger ("matrix ignoring redaction for unknown timeline message " <> renderEventId targetEventId)
                Just message -> do
                  state <- getMessageState observer.bridge.timeline message
                  case state of
                    Nothing -> logDebug observer.bridge.logger ("matrix ignoring redaction for unknown timeline message " <> renderEventId targetEventId)
                    Just current -> do
                      let timelineEvent =
                            Event
                              { platformKey = matrixRedactionKey platformRoom event.eventId
                              , content = RetractMessage current.messageHead
                              }
                      submitted <- submitMapped observer.bridge.logger observer.bridge.timeline observer.bridge.mappingStore timelineEvent
                      case submitted of
                        Right _ -> pure ()
                        Left (MissingReferences missing)
                          | current.messageHead `elem` missing ->
                              logDebug observer.bridge.logger ("matrix ignoring redaction for missing timeline event " <> renderEventId current.messageHead)
                        Left err -> logError observer.bridge.logger ("matrix retraction submit error: " <> Text.pack (show err))
        MatrixMember member -> observeMatrixMember observer room event member
        MatrixRoomAvatar mxc -> observeMatrixRoomAvatar observer platformRoom room mxc
        MatrixIgnored reason
          | reason == "m.room.encrypted" ->
              logWarn observer.bridge.logger ("matrix cannot decrypt event " <> event.eventId <> ": encrypted rooms are not supported")
          | otherwise -> logDebug observer.bridge.logger ("matrix ignoring " <> reason <> " event " <> event.eventId)
 where
  isMember (MatrixMember _) = True
  isMember (MatrixRoomAvatar _) = True
  isMember _ = False

observeMatrixMessage :: Timeline timeline => Observer timeline -> MatrixEvent -> Text -> RoomId -> MatrixMessageContent -> IO ()
observeMatrixMessage observer event platformRoom room messageContent = do
  let context = observer.bridge
  userId <- ensureUser observer event.sender
  ensureRoomMembership context.logger context.timeline context.mappingStore "matrix" room userId
  body <- matrixObservedBody observer messageContent.messageBody
  case messageContent.messageEditOf of
    Just targetEventId -> do
      target <- context.mappingStore.lookupTimelineId (matrixEventKey platformRoom targetEventId)
      case target of
        Nothing -> logDebug context.logger ("matrix ignoring edit for unknown event " <> targetEventId)
        Just targetMessage -> do
          let platformKey = PlatformKey "matrix" "edit" (platformRoom <> "/" <> event.eventId)
          alreadyObserved <- context.mappingStore.lookupTimelineId platformKey
          unless (isJust alreadyObserved) do
            root <- resolveMessageId context.timeline targetMessage
            case root of
              Nothing -> logDebug context.logger ("matrix ignoring edit for unknown timeline message " <> renderEventId targetMessage)
              Just message -> do
                state <- getMessageState context.timeline message
                case state of
                  Nothing -> logDebug context.logger ("matrix ignoring edit for unknown timeline message " <> renderEventId targetMessage)
                  Just current -> do
                    if current.messageBody == Just body || isNothing current.messageBody
                      then context.mappingStore.insertMapping platformKey (messageEventId message)
                      else do
                        let timelineEvent =
                              Event
                                { platformKey
                                , content = EditMessage current.messageHead body
                                }
                        submitted <- submitMapped context.logger context.timeline context.mappingStore timelineEvent
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
                      , body
                      }
              }
      submitted <- submitMessageMapped context.logger context.timeline context.mappingStore timelineEvent
      case submitted of
        Right _ -> pure ()
        Left err -> logError context.logger ("matrix submit error: " <> Text.pack (show err))

-- | Matrix mentions are links to @matrix.to@. A user link names a user the
-- timeline can create, while room and message links only become mentions when
-- the bridge already mapped the target.
matrixObservedBody :: Timeline timeline => Observer timeline -> Message -> IO Message
matrixObservedBody observer = mapMessageInlines \case
  Link label url -> case matrixLinkTarget url of
    Just (MatrixUserTarget matrixUserId) -> do
      let name = if Text.null (inlineToText label) then matrixUserId else inlineToText label
      mapped <-
        ensureUserMapped
          context.logger
          context.timeline
          context.mappingStore
          (matrixUserKey matrixUserId)
          (pure UserInfo {name, avatar = Nothing})
      case mapped of
        Right user -> pure (Mention label (userEventId user))
        Left err -> do
          logDebug context.logger ("matrix mention user submit error: " <> Text.pack (show err))
          pure (Link label url)
    Just (MatrixRoomTarget roomId) -> do
      mapped <- context.mappingStore.lookupTimelineId (matrixRoomKey roomId)
      pure (maybe (Link label url) (Mention label) mapped)
    Just (MatrixMessageTarget roomId messageId) -> do
      mapped <- context.mappingStore.lookupTimelineId (matrixEventKey roomId messageId)
      pure (maybe (Link label url) (Mention label) mapped)
    Nothing -> pure (Link label url)
  part -> pure part
 where
  context = observer.bridge

data MatrixLinkTarget
  = MatrixUserTarget Text
  | MatrixRoomTarget Text
  | MatrixMessageTarget Text Text

matrixLinkTarget :: Text -> Maybe MatrixLinkTarget
matrixLinkTarget url = do
  rest <- Text.stripPrefix "https://matrix.to/#/" url
  case Text.splitOn "/" (Text.takeWhile (/= '?') rest) of
    [recipient]
      | "@" `Text.isPrefixOf` recipient -> Just (MatrixUserTarget recipient)
      | "!" `Text.isPrefixOf` recipient -> Just (MatrixRoomTarget recipient)
    [recipient, matrixEventId]
      | "!" `Text.isPrefixOf` recipient, "$" `Text.isPrefixOf` matrixEventId ->
          Just (MatrixMessageTarget recipient matrixEventId)
    _ -> Nothing

observeMatrixMedia :: Timeline timeline => Observer timeline -> MatrixEvent -> Text -> RoomId -> MatrixMediaContent -> IO ()
observeMatrixMedia observer event platformRoom room mediaContent = do
  let context = observer.bridge
  userId <- ensureUser observer event.sender
  ensureRoomMembership context.logger context.timeline context.mappingStore "matrix" room userId
  let blobKey = matrixBlobKey mediaContent.mediaUri
  existing <- context.mappingStore.lookupTimelineId blobKey
  blobId <- case existing of
    Just blobEvent -> pure (Just (BlobId blobEvent))
    Nothing -> do
      downloaded <- matrixDownloadMedia context mediaContent.mediaUri
      case downloaded of
        Nothing -> pure Nothing
        Just bytes -> do
          submitted <- submitBlob context.logger context.timeline context.mappingStore blobKey bytes
          case submitted of
            Right blob -> pure (Just blob)
            Left err -> do
              logError context.logger ("matrix media submit error: " <> Text.pack (show err))
              pure Nothing
  let label = fromMaybe (matrixMediaLabel mediaContent.mediaKind) mediaContent.mediaName
      parts = case blobId of
        Just blob
          | mediaContent.mediaKind == FileBlob -> [Text (plainInline label), Blob FileBlob blob]
          | otherwise -> [Blob mediaContent.mediaKind blob]
        Nothing -> [Text (plainInline ("[" <> label <> "]"))]
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
      submitted <- submitMessageMapped context.logger context.timeline context.mappingStore timelineEvent
      case submitted of
        Right _ -> pure ()
        Left err -> logError context.logger ("matrix media message submit error: " <> Text.pack (show err))

matrixMediaLabel :: BlobType -> Text
matrixMediaLabel = \case
  ImageBlob -> "image"
  VideoBlob -> "video"
  AudioBlob -> "audio"
  FileBlob -> "file"

observeMatrixMember :: Timeline timeline => Observer timeline -> RoomId -> MatrixEvent -> MatrixMembership -> IO ()
observeMatrixMember observer room event member
  | member.memberUserId == observer.ownUserId =
      logDebug observer.bridge.logger ("matrix ignoring own membership " <> member.memberUserId)
  | otherwise = case member.membership of
      "join" -> withUser \user -> do
        submitMembership "join" user
        syncUserProfile context.logger context.timeline context.mappingStore (matrixUserKey member.memberUserId) user (matrixUserProfile context member.memberUserId)
        roomInfo <- matrixRoomUserInfo context user room member
        submitUserPerRoom context.logger context.timeline context.mappingStore (eventKey "user-room") user room roomInfo
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
    result <- submitMapped context.logger context.timeline context.mappingStore Event {platformKey, content}
    case result of
      Left err -> logError context.logger ("matrix membership submit error: " <> Text.pack (show err))
      Right _ -> pure ()

  submitMembership kind user = do
    previous <- getMembership context.timeline user room
    let content = if kind == "join" then JoinRoom user room previous else LeaveRoom user room previous
    submitEvent (eventKey kind) content

matrixRoomUserInfo :: Timeline timeline => Context timeline -> UserId -> RoomId -> MatrixMembership -> IO (Maybe UserInfo)
matrixRoomUserInfo context user room member
  | isNothing displayName, isNothing avatarUrl = pure Nothing
  | otherwise = do
      base <- getUserInfo context.timeline user (Just room)
      avatar <- case avatarUrl of
        Nothing -> pure (base >>= \info -> info.avatar)
        Just mxc -> do
          result <- matrixAvatarBlob context mxc
          pure case result of
            Just (Just blob) -> Just blob
            _ -> base >>= \info -> info.avatar
      let fallbackName = maybe (renderEventId (userEventId user)) (.name) base
          name = fromMaybe fallbackName displayName
      pure (Just (UserInfo name avatar))
 where
  displayName = member.memberDisplayName >>= nonEmptyText
  avatarUrl = member.memberAvatarUrl >>= nonEmptyText

  nonEmptyText value
    | Text.null value = Nothing
    | otherwise = Just value

observeMatrixRoomAvatar :: Timeline timeline => Observer timeline -> Text -> RoomId -> Text -> IO ()
observeMatrixRoomAvatar observer platformRoom room mxc = do
  let context = observer.bridge
  origin <- roomOriginPlatform context.timeline room
  when (origin == Just "matrix") do
    current <- getRoomInfo context.timeline room
    case current of
      Nothing -> pure ()
      Just (RoomInfo currentName currentDescription _) -> do
        avatarResult <- matrixAvatarBlob context mxc
        case avatarResult of
          Nothing -> pure ()
          Just avatar -> do
            let updated = RoomInfo currentName currentDescription avatar
            syncRoomProfile context.logger context.timeline context.mappingStore (matrixRoomKey platformRoom) room (pure (Just updated))

matrixReplyTo :: Timeline timeline => Context timeline -> Text -> Maybe Text -> IO (Maybe MessageId)
matrixReplyTo context platformRoom replyToEvent =
  case replyToEvent of
    Nothing -> pure Nothing
    Just repliedEvent -> do
      mapped <- context.mappingStore.lookupTimelineId (matrixEventKey platformRoom repliedEvent)
      case mapped of
        Nothing -> pure Nothing
        Just target -> resolveMessageId context.timeline target

ensureUser :: Timeline timeline => Observer timeline -> Text -> IO UserId
ensureUser observer matrixUserId = do
  let context = observer.bridge
  mapped <-
    ensureUserMapped
      context.logger
      context.timeline
      context.mappingStore
      (matrixUserKey matrixUserId)
      (matrixUserInfoForCreate context matrixUserId)
  either (\err -> fail ("could not create matrix user: " <> show err)) pure mapped

reflectEvent :: Timeline timeline => Context timeline -> StoredEvent -> IO ()
reflectEvent context stored =
  case stored.storedEvent.content of
    SendMessage info
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId info.room)
          mapM_ (sendToRoom info) (filter (enabledRoom context.config.initialRooms) roomKeys)
    EditMessage previous body
      | stored.storedEvent.platformKey.platform /= "matrix" -> do
          root <- getMessageRoot context.timeline previous
          case root of
            Nothing -> logError context.logger ("matrix edit references unknown message " <> renderEventId previous)
            Just message -> do
              original <- lookupSendMessage context.timeline message
              case original of
                Just info -> do
                  unless (messageBlobs info.body == messageBlobs body) do
                    logWarn context.logger "matrix cannot reflect changes to message attachments"
                  targets <- matrixEditTargets context message
                  alreadyMapped <- hasPlatformMapping context.mappingStore "matrix" stored.storedId
                  unless alreadyMapped do
                    forM_ targets \(roomId, matrixEventId) -> do
                      resolved <- matrixReflectedBody context roomId (textBody body)
                      (profile, text) <- matrixRelayBody context info (renderMessageBody resolved)
                      let txnId = matrixTransactionId stored "edit" roomId
                      result <- matrixSendEdit context roomId txnId matrixEventId profile text
                      case result of
                        Left err -> logError context.logger ("matrix edit reflect error: " <> Text.pack err)
                        Right editEventId -> do
                          let platformKey = matrixEventKey roomId editEventId
                          context.mappingStore.insertMapping platformKey stored.storedId
                          logReflected context.logger stored platformKey
                _ -> pure ()
    RetractMessage previous
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin event " <> renderEventId stored.storedId)
      | otherwise -> do
          root <- getMessageRoot context.timeline previous
          case root of
            Nothing -> logError context.logger ("matrix retraction references unknown message " <> renderEventId previous)
            Just message -> do
              alreadyMapped <- hasPlatformMapping context.mappingStore "matrix" stored.storedId
              unless alreadyMapped do
                target <- lookupSendMessage context.timeline message
                targets <- matrixRetractTargets context message
                mapM_ (redactInRoom target) (filter (enabledRoom context.config.initialRooms . matrixRoomKey . fst) targets)
    ModifyUserPerRoom user room info previous
      | stored.storedEvent.platformKey.platform == "matrix" ->
          logDebug context.logger ("matrix not reflecting own-origin room profile change " <> renderEventId stored.storedId)
      | otherwise -> do
          body <- renderUserPerRoomChange context.timeline user room previous info
          forM_ body \rendered -> do
            roomKeys <- context.mappingStore.lookupPlatformKeys (roomEventId room)
            forM_ (filter (enabledRoom context.config.initialRooms) roomKeys) \roomKey -> do
              alreadySent <- matrixMessageMappedToRoom context stored.storedId roomKey.key
              unless alreadySent do
                let txnId = matrixTransactionId stored "nick" roomKey.key
                result <- matrixSendMessage context roomKey.key txnId Nothing Nothing rendered
                case result of
                  Left err -> logError context.logger ("matrix room profile reflect error: " <> Text.pack err)
                  Right matrixEventId -> do
                    let platformKey = matrixEventKey roomKey.key matrixEventId
                    context.mappingStore.insertMapping platformKey stored.storedId
                    logReflected context.logger stored platformKey
    _ -> pure ()
 where
  sendToRoom info roomKey = do
    alreadySent <- matrixMessageMappedToRoom context stored.storedId roomKey.key
    unless alreadySent do
      resolved <- matrixReflectedBody context roomKey.key (textBody info.body)
      (profile, body) <- matrixRelayBody context info (renderMessageBody resolved)
      replyTo <- matrixReplyEventId context roomKey.key info.replyTo
      let roomId = roomKey.key
          blobs = messageBlobs info.body
          sendPart action = do
            result <- action
            case result of
              Left err -> logError context.logger ("matrix reflect error: " <> Text.pack err)
              Right matrixEventId -> do
                let platformKey = matrixEventKey roomId matrixEventId
                context.mappingStore.insertMapping platformKey stored.storedId
                logReflected context.logger stored platformKey
          sendText = sendPart (matrixSendMessage context roomId (matrixTransactionId stored "text" roomId) replyTo (Just profile) body)
          sendBlob index (kind, blobId) = do
            blob <- getEvent context.timeline (blobEventId blobId)
            case fmap (.storedEvent.content) blob of
              Just (CreateBlob bytes) -> do
                let name = "file-" <> Text.take 12 (renderEventId (blobEventId blobId)) <> sniffExtension bytes
                    mimeType = sniffMimeType bytes
                cached <- matrixUploadedMedia context blobId
                uploaded <- case cached of
                  Just mediaUrl -> pure (Right mediaUrl)
                  Nothing -> matrixUploadMedia context name mimeType bytes
                case uploaded of
                  Left err -> logError context.logger ("matrix media upload error: " <> Text.pack err)
                  Right mediaUrl -> do
                    context.mappingStore.insertMapping (matrixBlobKey mediaUrl) (blobEventId blobId)
                    let blobReplyTo = if index == 1 && not (hasTextContent info.body) then replyTo else Nothing
                        content = matrixMediaContent (Just profile) kind name mimeType (ByteString.length bytes) mediaUrl
                        partTxnId = matrixTransactionId stored ("part" <> Text.pack (show index)) roomId
                    sendPart (matrixSendMedia context roomId partTxnId blobReplyTo content)
              _ -> logError context.logger "matrix media blob is missing"
      when (hasTextContent info.body) sendText
      forM_ (zip [1 :: Int ..] blobs) (uncurry sendBlob)
  redactInRoom target (roomId, eventId_) = do
    let txnId = matrixTransactionId stored "redact" roomId
    result <- matrixRedactMessage context roomId txnId eventId_
    case result of
      Left err -> do
        logError context.logger ("matrix retract reflect error: " <> Text.pack err)
        let fallbackTxnId = matrixTransactionId stored "redact-failure" roomId
        (profile, notice) <- case target of
          Just info -> do
            (targetProfile, prefixed) <- matrixRelayBody context info (retractionFailureMessage err)
            pure (Just targetProfile, prefixed)
          Nothing -> pure (Nothing, retractionFailureMessage err)
        fallback <- matrixSendMessage context roomId fallbackTxnId Nothing profile notice
        case fallback of
          Left fallbackErr -> logError context.logger ("matrix retract failure notice error: " <> Text.pack fallbackErr)
          Right matrixEventId -> do
            let platformKey = matrixEventKey roomId matrixEventId
            context.mappingStore.insertMapping platformKey stored.storedId
            logReflected context.logger stored platformKey
      Right redactionEventId -> do
        let mappingKey =
              maybe
                (PlatformKey "matrix" "redaction" (roomId <> "/" <> renderEventId stored.storedId))
                (matrixRedactionKey roomId)
                redactionEventId
        context.mappingStore.insertMapping mappingKey stored.storedId
        logReflected context.logger stored mappingKey

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
  keys <- lookupMappedKeys context.mappingStore "matrix" "message" eventId_
  pure (mapMaybe matrixMessageTarget keys)

matrixMessageTarget :: Text -> Maybe (Text, Text)
matrixMessageTarget key =
  case Text.splitOn "/" key of
    [roomId, matrixEventId] -> Just (roomId, matrixEventId)
    _ -> Nothing

matrixMessageMappedToRoom :: Context timeline -> EventId -> Text -> IO Bool
matrixMessageMappedToRoom context eventId_ roomId = do
  targets <- matrixMessageTargets context eventId_
  pure (any ((== roomId) . fst) targets)

-- | Media this bridge already uploaded for a blob, so that a blob reflected into
-- several rooms is uploaded once. The mapper is keyed by the URI the homeserver
-- returned, which is what the observer would record for the same media anyway.
matrixUploadedMedia :: Context timeline -> BlobId -> IO (Maybe Text)
matrixUploadedMedia context blobId =
  listToMaybe <$> lookupMappedKeys context.mappingStore "matrix" "blob" (blobEventId blobId)

-- | Upload a timeline avatar once and reuse the resulting mxc URI for every
-- message from that user.
matrixAvatarUrl :: Timeline timeline => Context timeline -> Maybe BlobId -> IO (Maybe Text)
matrixAvatarUrl _ Nothing = pure Nothing
matrixAvatarUrl context (Just blobId) = do
  cached <- matrixUploadedMedia context blobId
  case cached of
    Just mediaUrl -> pure (Just mediaUrl)
    Nothing -> do
      bytes <- lookupBlobBytes context.timeline blobId
      case bytes of
        Nothing -> do
          logWarn context.logger ("matrix avatar blob is missing " <> renderEventId (blobEventId blobId))
          pure Nothing
        Just content -> do
          let name = "avatar-" <> Text.take 12 (renderEventId (blobEventId blobId)) <> sniffExtension content
              mimeType = sniffMimeType content
          uploaded <- matrixUploadMedia context name mimeType content
          case uploaded of
            Left err -> do
              logWarn context.logger ("matrix avatar upload error: " <> Text.pack err)
              pure Nothing
            Right mediaUrl -> do
              context.mappingStore.insertMapping (matrixBlobKey mediaUrl) (blobEventId blobId)
              pure (Just mediaUrl)

matrixReplyEventId :: Context timeline -> Text -> Maybe MessageId -> IO (Maybe Text)
matrixReplyEventId _ _ Nothing = pure Nothing
matrixReplyEventId context roomId (Just message) = do
  targets <- matrixMessageTargets context (messageEventId message)
  pure (lookup roomId targets)

matrixCall :: FromJSON a => Context timeline -> Text -> (Request -> Request) -> IO (Either String a)
matrixCall context url modify = do
  request0 <- authorizedRequest context url
  response <- loggedHttp context.logger (modify request0) context.manager
  pure (eitherDecode response.responseBody)

matrixSync :: Context timeline -> Maybe Text -> IO (Either String MatrixSync)
matrixSync context since = do
  let query =
        [ ("timeout", Just (Text.Encoding.encodeUtf8 (Text.pack (show context.config.syncTimeoutMs))))
        ]
          <> maybe [] (\token -> [("since", Just (Text.Encoding.encodeUtf8 token))]) since
  matrixCall context (context.config.homeserver <> "/_matrix/client/v3/sync") (setQueryString query)

matrixWhoami :: Context timeline -> IO (Either String MatrixWhoami)
matrixWhoami context =
  matrixCall context (context.config.homeserver <> "/_matrix/client/v3/account/whoami") id

matrixGetDisplayName :: Context timeline -> Text -> IO (Either String MatrixDisplayName)
matrixGetDisplayName context matrixUserId =
  matrixCall
    context
    (context.config.homeserver <> "/_matrix/client/v3/profile/" <> escapePathSegment matrixUserId <> "/displayname")
    id

data MatrixAvatar = MatrixAvatar
  { avatarUrl :: Maybe Text
  }

instance FromJSON MatrixAvatar where
  parseJSON = withObject "MatrixAvatar" \obj ->
    MatrixAvatar <$> obj .:? "avatar_url"

matrixGetAvatarUrl :: Context timeline -> Text -> IO (Either String MatrixAvatar)
matrixGetAvatarUrl context matrixUserId =
  matrixCall
    context
    (context.config.homeserver <> "/_matrix/client/v3/profile/" <> escapePathSegment matrixUserId <> "/avatar_url")
    id

matrixUserInfoForCreate :: Timeline timeline => Context timeline -> Text -> IO UserInfo
matrixUserInfoForCreate context matrixUserId = do
  displayResult <- matrixGetDisplayName context matrixUserId
  let name = case displayResult of
        Right display -> fromMaybe matrixUserId display.displayName
        Left _ -> matrixUserId
  avatar <- matrixUserAvatar context matrixUserId
  pure UserInfo {name, avatar}

matrixUserAvatar :: Timeline timeline => Context timeline -> Text -> IO (Maybe BlobId)
matrixUserAvatar context matrixUserId = do
  avatarResult <- matrixGetAvatarUrl context matrixUserId
  case avatarResult of
    Left _ -> pure Nothing
    Right avatarInfo -> case avatarInfo.avatarUrl of
      Nothing -> pure Nothing
      Just mxc -> do
        result <- matrixAvatarBlob context mxc
        pure (result >>= id)

-- | A profile fetch returns 'Nothing' on lookup failure, so synchronization
-- never clears an avatar just because the homeserver was unreachable.
matrixUserProfile :: Timeline timeline => Context timeline -> Text -> IO (Maybe UserInfo)
matrixUserProfile context matrixUserId = do
  displayResult <- matrixGetDisplayName context matrixUserId
  avatarResult <- matrixGetAvatarUrl context matrixUserId
  case (displayResult, avatarResult) of
    (Right display, Right avatarInfo) -> do
      avatar <- case avatarInfo.avatarUrl of
        Nothing -> pure (Just Nothing)
        Just mxc -> matrixAvatarBlob context mxc
      pure do
        blob <- avatar
        pure
          UserInfo
            { name = fromMaybe matrixUserId display.displayName
            , avatar = blob
            }
    (Left err, _) -> do
      logDebug context.logger ("matrix display name lookup failed for " <> matrixUserId <> ": " <> Text.pack err)
      pure Nothing
    (_, Left err) -> do
      logDebug context.logger ("matrix avatar lookup failed for " <> matrixUserId <> ": " <> Text.pack err)
      pure Nothing

-- | The outer 'Maybe' is fetch success; the inner one is whether the avatar
-- exists. That distinction lets a failed download preserve the current avatar.
matrixAvatarBlob :: Timeline timeline => Context timeline -> Text -> IO (Maybe (Maybe BlobId))
matrixAvatarBlob context mxc = do
  existing <- context.mappingStore.lookupTimelineId (matrixBlobKey mxc)
  case existing of
    Just eventId -> pure (Just (Just (BlobId eventId)))
    Nothing -> do
      downloaded <- matrixDownloadMedia context mxc
      case downloaded of
        Nothing -> pure Nothing
        Just bytes -> do
          imported <- importAvatar context.logger context.timeline context.mappingStore (matrixBlobKey mxc) bytes
          pure (Just imported)

-- | Mentions become platform links when the target exists in the room the
-- message is being reflected into; otherwise the mention keeps its label text.
matrixReflectedBody :: Timeline timeline => Context timeline -> Text -> Message -> IO Message
matrixReflectedBody context platformRoom = mapMessageInlines \case
  Mention label target -> do
    url <- matrixMentionUrl context platformRoom target
    pure (maybe (Mention label target) (Link label) url)
  part -> pure part

matrixMentionUrl :: Timeline timeline => Context timeline -> Text -> EventId -> IO (Maybe Text)
matrixMentionUrl context platformRoom target = do
  stored <- getEvent context.timeline target
  case fmap (.storedEvent.content) stored of
    Just (CreateUser _) -> matrixEntityUrl "user" target
    Just (SendMessage _) -> do
      keys <- lookupMappedKeys context.mappingStore "matrix" "message" target
      let matching =
            listToMaybe
              [ roomTarget
              | key <- keys
              , Just roomTarget <- [matrixMessageTarget key]
              , fst roomTarget == platformRoom
              ]
      pure case matching of
        Just (roomId, messageId) -> Just ("https://matrix.to/#/" <> roomId <> "/" <> messageId)
        Nothing -> Nothing
    Just (CreateRoom _) -> matrixEntityUrl "room" target
    _ -> pure Nothing
 where
  matrixEntityUrl entityType target_ = do
    keys <- lookupMappedKeys context.mappingStore "matrix" entityType target_
    pure (("https://matrix.to/#/" <>) <$> listToMaybe keys)

-- | Attaches the sender's MSC4144 profile to a message body. Edits and retraction
-- notices reuse the profile of the message they act on, so a relayed message keeps
-- the sender it was originally given. The name is repeated in the body because the
-- profile key is unstable and clients without it must still see who sent this.
matrixRelayBody :: Timeline timeline => Context timeline -> SendMessageInfo -> RenderedMessage -> IO (Value, RenderedMessage)
matrixRelayBody context info body = do
  user <- getUserInfo context.timeline info.sender (Just info.room)
  avatarUrl <- matrixAvatarUrl context (user >>= (.avatar))
  let displayName = matrixProfileDisplayName (maybe "unknown" (.name) user)
      profile =
        object
          [ "id" .= renderEventId (userEventId info.sender)
          , "displayname" .= displayName
          , "avatar_url" .= fromMaybe "" avatarUrl
          ]
      fallback =
        RenderedMessage
          { plainText = displayName <> ": " <> body.plainText
          , htmlText =
              "<strong data-mx-profile-fallback>"
                <> htmlEscape displayName
                <> ": </strong>"
                <> body.htmlText
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
    nextBytes = bytes + ByteString.length (Text.Encoding.encodeUtf8 (Text.singleton char))

matrixSendMessage ::
  Context timeline -> Text -> Text -> Maybe Text -> Maybe Value -> RenderedMessage -> IO (Either String Text)
matrixSendMessage context roomId txnId replyTo profile body =
  matrixSendContent context roomId txnId (matrixMessageContent replyTo profile body)

matrixSendEdit :: Context timeline -> Text -> Text -> Text -> Value -> RenderedMessage -> IO (Either String Text)
matrixSendEdit context roomId txnId targetEventId profile body =
  matrixSendContent context roomId txnId (matrixEditContent targetEventId profile body)

matrixSendMedia :: Context timeline -> Text -> Text -> Maybe Text -> Value -> IO (Either String Text)
matrixSendMedia context roomId txnId replyTo content =
  matrixSendContent context roomId txnId (matrixWithReply replyTo content)

matrixSendContent :: Context timeline -> Text -> Text -> Value -> IO (Either String Text)
matrixSendContent context roomId txnId content = do
  result <-
    matrixCall
      context
      ( context.config.homeserver
          <> "/_matrix/client/v3/rooms/"
          <> roomId
          <> "/send/m.room.message/"
          <> txnId
      )
      ( \request ->
          request
            { method = "PUT"
            , requestBody = RequestBodyLBS (encode content)
            , requestHeaders = ("Content-Type", "application/json") : request.requestHeaders
            }
      ) :: IO (Either String MatrixSendResponse)
  pure (fmap (.eventId) result)

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
              , requestHeaders = ("Content-Type", Text.Encoding.encodeUtf8 mimeType) : request0.requestHeaders
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

matrixTextContent :: RenderedMessage -> Value
matrixTextContent body =
  object
    [ "msgtype" .= ("m.text" :: Text)
    , "body" .= body.plainText
    , "format" .= ("org.matrix.custom.html" :: Text)
    , "formatted_body" .= body.htmlText
    ]

matrixMessageContent :: Maybe Text -> Maybe Value -> RenderedMessage -> Value
matrixMessageContent replyTo profile body =
  matrixWithProfile profile True (matrixWithReply replyTo (matrixTextContent body))

matrixEditContent :: Text -> Value -> RenderedMessage -> Value
matrixEditContent targetEventId profile body =
  matrixWithProfile (Just profile) True
    $ insertField "m.new_content" (matrixTextContent body)
    $ insertField "m.relates_to" relates
    $ prefixed
 where
  relates = object ["rel_type" .= ("m.replace" :: Text), "event_id" .= targetEventId]
  prefixed =
    object
      [ "msgtype" .= ("m.text" :: Text)
      , "body" .= ("* " <> body.plainText)
      , "format" .= ("org.matrix.custom.html" :: Text)
      , "formatted_body" .= ("* " <> body.htmlText)
      ]

matrixMediaContent :: Maybe Value -> BlobType -> Text -> Text -> Int -> Text -> Value
matrixMediaContent profile kind fileName mimeType size mediaUrl =
  matrixWithProfile profile False
    $ object
      [ "msgtype" .= matrixMsgType kind
      , "body" .= fileName
      , "url" .= mediaUrl
      , "info" .= object ["mimetype" .= mimeType, "size" .= size]
      ]

-- | MSC4144 specifies the unstable key until the proposal is accepted.
-- @has_fallback@ promises that the sender name is also present in the content as
-- text; only a text body has room for it, so media sends claim no fallback.
matrixWithProfile :: Maybe Value -> Bool -> Value -> Value
matrixWithProfile Nothing _ content = content
matrixWithProfile (Just profile) hasFallback content =
  insertField "com.beeper.per_message_profile" (withFallback profile) content
 where
  withFallback = \case
    Object profileFields -> Object (Aeson.KeyMap.insert "has_fallback" (Bool hasFallback) profileFields)
    other -> other

insertField :: Aeson.Key.Key -> Value -> Value -> Value
insertField key value = \case
  Object fields -> Object (Aeson.KeyMap.insert key value fields)
  other -> other

matrixMsgType :: BlobType -> Text
matrixMsgType = \case
  ImageBlob -> "m.image"
  VideoBlob -> "m.video"
  AudioBlob -> "m.audio"
  FileBlob -> "m.file"

matrixWithReply :: Maybe Text -> Value -> Value
matrixWithReply Nothing content = content
matrixWithReply (Just eventId_) content =
  insertField "m.relates_to" (object ["m.in_reply_to" .= object ["event_id" .= eventId_]]) content

data MatrixUploadResponse = MatrixUploadResponse
  { contentUri :: Text
  }

instance FromJSON MatrixUploadResponse where
  parseJSON = withObject "MatrixUploadResponse" \obj ->
    MatrixUploadResponse <$> obj .: "content_uri"

fetchInitialRoomInfo :: Timeline timeline => Context timeline -> InitialRoomMapping -> IO RoomInfo
fetchInitialRoomInfo context mapping = do
  name <- matrixStateField context mapping.platformKey.key "m.room.name" "name"
  topic <- matrixStateField context mapping.platformKey.key "m.room.topic" "topic"
  avatarUrl <- matrixStateField context mapping.platformKey.key "m.room.avatar" "url"
  avatar <- case avatarUrl of
    Nothing -> pure Nothing
    Just mxc -> do
      result <- matrixAvatarBlob context mxc
      pure (result >>= id)
  pure
    RoomInfo
      { name = maybe mapping.platformKey.key id name
      , description = fromMaybe "" topic
      , avatar
      }

matrixStateField :: Context timeline -> Text -> Text -> Text -> IO (Maybe Text)
matrixStateField context roomId eventType field = do
  result <- matrixCall context url id :: IO (Either String Value)
  pure do
    value <- either (const Nothing) Just result
    obj <- asObject value
    Aeson.KeyMap.lookup (Aeson.Key.fromText field) obj >>= asText
 where
  url = context.config.homeserver <> "/_matrix/client/v3/rooms/" <> roomId <> "/state/" <> eventType

loggedHttp :: Logger -> Request -> Manager -> IO (Response LByteString.ByteString)
loggedHttp logger = HTTP.loggedHttp logger matrixHttpLogOptions

matrixHttpLogOptions :: HttpLogOptions
matrixHttpLogOptions =
  defaultHttpLogOptions {requestSuffix = " authorization=<redacted>"}

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
      { requestHeaders = ("Authorization", Text.Encoding.encodeUtf8 ("Bearer " <> context.config.accessToken)) : request.requestHeaders
      }

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

matrixBlobKey :: Text -> PlatformKey
matrixBlobKey mediaUri = PlatformKey "matrix" "blob" mediaUri

escapePathSegment :: Text -> Text
escapePathSegment = Text.Encoding.decodeUtf8 . urlEncode True . Text.Encoding.encodeUtf8

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
  | MatrixRoomAvatar Text
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
  , memberAvatarUrl :: Maybe Text
  , memberPreviousAvatarUrl :: Maybe Text
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
          previousAvatarUrl = previousContent >>= Aeson.KeyMap.lookup "avatar_url" >>= asText
      event
        ( MatrixMember
            MatrixMembership
              { membership
              , memberUserId
              , memberDisplayName = Aeson.KeyMap.lookup "displayname" content >>= asText
              , memberPreviousDisplayName = previousDisplayName
              , memberAvatarUrl = Aeson.KeyMap.lookup "avatar_url" content >>= asText
              , memberPreviousAvatarUrl = previousAvatarUrl
              }
        )
    "m.room.avatar" -> do
      content <- eventContent
      MatrixEvent matrixEventId sender . MatrixRoomAvatar <$> (Aeson.KeyMap.lookup "url" content >>= asText)
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
        Just url -> Link (plainInline label) url
        Nothing -> Plain label
   in pure (Text (pure inline))

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
            Nothing -> (Text (plainInline ""), after)
  | otherwise =
      case headingLevel start of
        Just level ->
          let tagName = "h" <> Text.pack (show level)
              (inside, after) = takeUntilClose tagName rest
           in (Heading level (matrixInlineFromTagsOrPlain inside), after)
        Nothing -> (Text (plainInline (innerText [start])), rest)

messageFromTags :: [Tag Text] -> Message
messageFromTags tags =
  case parseMessageParts tags of
    part : parts -> part :| parts
    [] -> plainMessage (innerText tags)

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
    Nothing -> plainInline (innerText tags)

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
