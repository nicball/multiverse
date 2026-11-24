module Multiverse.Bridge
  ( Bridge (..)
  , BridgeRuntime (..)
  , enabledRoom
  , lookupConfiguredRoom
  , lookupMappedKeys
  , MappingStore (..)
  , RenderedMessage (..)
  , hasPlatformMapping
  , renderRelayedMessage
  , renderMessageBody
  , renderUserPerRoomChange
  , mapMessageInlines
  , htmlEscape
  , ensureRoomMembership
  , ensureUserMapped
  , lookupSendMessage
  , resolveMessageId
  , lookupBlobBytes
  , importAvatar
  , modifyUserKey
  , modifyUserPerRoomKey
  , modifyRoomKey
  , roomOriginPlatform
  , syncUserProfile
  , syncUserPerRoom
  , syncRoomProfile
  , submitUserPerRoom
  , submitMapped
  , submitMessageMapped
  , submitBlob
  , logObserved
  , logReflected
  , openMappingStore
  , ensureInitialRooms
  , startBridge
  , reflectForever
  , textBody
  , messageBlobs
  , hasTextContent
  , sniffExtension
  , sniffMimeType
  , isAnimatedImage
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (forkIO, newMVar, threadDelay, withMVar)
import Control.Exception (SomeException, try)
import Control.Monad (forever, unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (nonEmpty, toList)
import Data.Text (Text)
import Data.Text qualified as Text
import Database.SQLite.Simple
import Multiverse.Config (InitialRoomMapping (..))
import Multiverse.Event hiding (eventId)
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data Bridge = Bridge
  { bridgeName :: Text
  , observe :: IO ()
  , reflect :: IO ()
  }

data BridgeRuntime timeline = BridgeRuntime
  { timeline :: timeline
  , logger :: Logger
  }

startBridge :: Logger -> Bridge -> IO ()
startBridge logger bridge = do
  startComponent logger (bridge.bridgeName <> " observer") bridge.observe
  startComponent logger (bridge.bridgeName <> " reflector") bridge.reflect

startComponent :: Logger -> Text -> IO () -> IO ()
startComponent logger name component =
  void (forkIO (forever runOnce))
 where
  runOnce = do
    result <- try component :: IO (Either SomeException ())
    case result of
      Left err -> logError logger (name <> " crashed, restarting: " <> Text.pack (show err))
      Right () -> logWarn logger (name <> " stopped, restarting")
    threadDelay 5000000

data MappingStore = MappingStore
  { lookupTimelineId :: PlatformKey -> IO (Maybe EventId)
  , lookupPlatformKeys :: EventId -> IO [PlatformKey]
  , insertMapping :: PlatformKey -> EventId -> IO ()
  , lookupState :: Text -> IO (Maybe Text)
  , setState :: Text -> Text -> IO ()
  }

enabledRoom :: [InitialRoomMapping] -> PlatformKey -> Bool
enabledRoom mappings key = any (\mapping -> mapping.enable && mapping.platformKey == key) mappings

lookupConfiguredRoom :: MappingStore -> [InitialRoomMapping] -> PlatformKey -> IO (Maybe RoomId)
lookupConfiguredRoom mappingStore mappings platformKey =
  if enabledRoom mappings platformKey
    then fmap RoomId <$> mappingStore.lookupTimelineId platformKey
    else pure Nothing

-- | The keys this timeline event was mapped to under one platform entity type.
lookupMappedKeys :: MappingStore -> Text -> Text -> EventId -> IO [Text]
lookupMappedKeys mappingStore platform entityType eventId = do
  keys <- mappingStore.lookupPlatformKeys eventId
  pure [key.key | key <- keys, key.platform == platform, key.entityType == entityType]

data RenderedMessage = RenderedMessage
  { plainText :: Text
  , htmlText :: Text
  }

hasPlatformMapping :: MappingStore -> Text -> EventId -> IO Bool
hasPlatformMapping store platform eventId = any ((== platform) . (.platform)) <$> store.lookupPlatformKeys eventId

renderRelayedMessage :: Timeline timeline => timeline -> SendMessageInfo -> IO RenderedMessage
renderRelayedMessage timeline info = do
  user <- getUserInfo timeline info.sender (Just info.room)
  let sender = maybe "unknown" (.name) user
      body = renderMessageBody info.body
  pure
    RenderedMessage
      { plainText = sender <> ": " <> body.plainText
      , htmlText = "<b>" <> htmlEscape sender <> ":</b> " <> body.htmlText
      }

renderMessageBody :: Message -> RenderedMessage
renderMessageBody message =
  let parts = map renderMessagePart (toList message)
   in RenderedMessage
        { plainText = Text.intercalate "\n" (map (.plainText) parts)
        , htmlText = Text.intercalate "\n" (map (.htmlText) parts)
        }

renderUserPerRoomChange :: Timeline timeline => timeline -> UserId -> RoomId -> EventId -> Maybe UserInfo -> IO (Maybe RenderedMessage)
renderUserPerRoomChange timeline user room previous newInfo = do
  previousInfo <- getUserInfoAt timeline user (Just room) previous
  globalInfo <- getUserInfo timeline user Nothing
  let previousName = maybe "unknown" (.name) previousInfo
      newName = maybe (maybe "unknown" (.name) globalInfo) (.name) newInfo
  pure
    if previousName == newName
      then Nothing
      else
        Just
          RenderedMessage
            { plainText = "* " <> previousName <> " is now known as " <> newName
            , htmlText = "<em>" <> htmlEscape previousName <> " is now known as " <> htmlEscape newName <> "</em>"
            }

renderMessagePart :: MessagePart -> RenderedMessage
renderMessagePart = \case
  Text inline -> renderInline inline
  Emote inline ->
    let rendered = renderInline inline
     in RenderedMessage
          { plainText = "_ " <> rendered.plainText
          , htmlText = "<em>" <> rendered.htmlText <> "</em>"
          }
  Blob _ _ -> RenderedMessage "[blob]" "[blob]"
  List items ->
    let rendered = map renderMessageBody (toList items)
     in RenderedMessage
          { plainText = Text.intercalate "\n" (map (("- " <>) . (.plainText)) rendered)
          , htmlText = "<ul>" <> Text.concat (map (("<li>" <>) . (<> "</li>") . (.htmlText)) rendered) <> "</ul>"
          }
  BlockQuote message ->
    let rendered = renderMessageBody message
     in RenderedMessage
          { plainText = "> " <> Text.replace "\n" "\n> " rendered.plainText
          , htmlText = "<blockquote>" <> rendered.htmlText <> "</blockquote>"
          }
  Heading level inline ->
    let rendered = renderInline inline
        clampedLevel = max 1 (min 6 level)
        tag = "h" <> Text.pack (show clampedLevel)
     in RenderedMessage
          { plainText = Text.replicate clampedLevel "#" <> " " <> rendered.plainText
          , htmlText = "<" <> tag <> ">" <> rendered.htmlText <> "</" <> tag <> ">"
          }

renderInline :: InlineText -> RenderedMessage
renderInline inline =
  let parts = map renderInlinePart (toList inline)
   in RenderedMessage
        { plainText = Text.concat (map (.plainText) parts)
        , htmlText = Text.concat (map (.htmlText) parts)
        }

renderInlinePart :: InlineTextPart -> RenderedMessage
renderInlinePart = \case
  Bold inline ->
    let rendered = renderInline inline
     in RenderedMessage rendered.plainText ("<strong>" <> rendered.htmlText <> "</strong>")
  Italic inline ->
    let rendered = renderInline inline
     in RenderedMessage rendered.plainText ("<em>" <> rendered.htmlText <> "</em>")
  Link inline url ->
    let rendered = renderInline inline
     in RenderedMessage
          { plainText = rendered.plainText <> " (" <> url <> ")"
          , htmlText = "<a href=\"" <> htmlEscape url <> "\">" <> rendered.htmlText <> "</a>"
          }
  Mention inline _ -> renderInline inline
  InlineQuote inline ->
    let rendered = renderInline inline
     in RenderedMessage ("\"" <> rendered.plainText <> "\"") ("<code>" <> rendered.htmlText <> "</code>")
  Plain text -> RenderedMessage text (htmlEscape text)

-- | Rewrite every inline part of a message, including the ones nested in
-- lists, quotes and headings. The transform sees a part after its own nested
-- inline text has been rewritten.
mapMessageInlines :: (InlineTextPart -> IO InlineTextPart) -> Message -> IO Message
mapMessageInlines transform = traverse messagePart
 where
  messagePart = \case
    Text inline -> Text <$> inlineText inline
    Blob kind blob -> pure (Blob kind blob)
    Emote inline -> Emote <$> inlineText inline
    List messages -> List <$> traverse (mapMessageInlines transform) messages
    BlockQuote message -> BlockQuote <$> mapMessageInlines transform message
    Heading level inline -> Heading level <$> inlineText inline

  inlineText = traverse inlinePart

  inlinePart given = do
    rebuilt <- case given of
      Bold inner -> Bold <$> inlineText inner
      Italic inner -> Italic <$> inlineText inner
      Link inner url -> Link <$> inlineText inner <*> pure url
      Mention inner target -> Mention <$> inlineText inner <*> pure target
      InlineQuote inner -> InlineQuote <$> inlineText inner
      Plain text -> pure (Plain text)
    transform rebuilt

htmlEscape :: Text -> Text
htmlEscape =
  Text.concatMap \case
    '&' -> "&amp;"
    '<' -> "&lt;"
    '>' -> "&gt;"
    '"' -> "&quot;"
    '\'' -> "&#39;"
    char -> Text.singleton char

textBody :: Message -> Message
textBody body = case nonEmpty (filter isText (toList body)) of
  Just parts -> parts
  Nothing -> plainMessage ""
 where
  isText (Blob _ _) = False
  isText _ = True

messageBlobs :: Message -> [(BlobType, BlobId)]
messageBlobs = concatMap collect . toList
 where
  collect = \case
    Blob kind blob -> [(kind, blob)]
    BlockQuote body -> messageBlobs body
    List items -> concatMap messageBlobs (toList items)
    _ -> []

hasTextContent :: Message -> Bool
hasTextContent = any isText . toList
 where
  isText (Blob _ _) = False
  isText _ = True

sniffExtension :: ByteString -> Text
sniffExtension = fst . sniffType

sniffMimeType :: ByteString -> Text
sniffMimeType = snd . sniffType

-- | GIF always animates; WebP only when it carries an @ANIM@ chunk.
isAnimatedImage :: ByteString -> Bool
isAnimatedImage bytes =
  "GIF8" `ByteString.isPrefixOf` bytes
    || ("RIFF" `ByteString.isPrefixOf` bytes && "ANIM" `ByteString.isInfixOf` ByteString.take 512 bytes)

sniffType :: ByteString -> (Text, Text)
sniffType bytes
  | "\137PNG" `ByteString.isPrefixOf` bytes = (".png", "image/png")
  | "\255\216\255" `ByteString.isPrefixOf` bytes = (".jpg", "image/jpeg")
  | "GIF8" `ByteString.isPrefixOf` bytes = (".gif", "image/gif")
  | "BM" `ByteString.isPrefixOf` bytes = (".bmp", "image/bmp")
  | "RIFF" `ByteString.isPrefixOf` bytes && ByteString.take 4 (ByteString.drop 8 bytes) == "WEBP" = (".webp", "image/webp")
  | "RIFF" `ByteString.isPrefixOf` bytes && ByteString.take 4 (ByteString.drop 8 bytes) == "WAVE" = (".wav", "audio/wav")
  | "ID3" `ByteString.isPrefixOf` bytes = (".mp3", "audio/mpeg")
  | "OggS" `ByteString.isPrefixOf` bytes = (".ogg", "audio/ogg")
  | "fLaC" `ByteString.isPrefixOf` bytes = (".flac", "audio/flac")
  | "#!AMR" `ByteString.isPrefixOf` bytes = (".amr", "audio/amr")
  | "\2#!SILK_V3" `ByteString.isPrefixOf` bytes = (".silk", "audio/silk")
  | "%PDF" `ByteString.isPrefixOf` bytes = (".pdf", "application/pdf")
  | "PK\3\4" `ByteString.isPrefixOf` bytes = (".zip", "application/zip")
  | "\31\139" `ByteString.isPrefixOf` bytes = (".gz", "application/gzip")
  | "ftyp" `ByteString.isPrefixOf` ByteString.drop 4 bytes = (".mp4", "video/mp4")
  | "\26\69\223\163" `ByteString.isPrefixOf` bytes = (".webm", "video/webm")
  | otherwise = ("", "application/octet-stream")

-- | Bridges learn about users lazily, so the first sighting of a user in a
-- room also establishes their membership. The last join/leave event for the
-- pair decides whether the user is currently in the room; a new join links to
-- it, so the membership events of a @(user, room)@ pair form a linked list.
ensureRoomMembership :: Timeline timeline => Logger -> timeline -> MappingStore -> Text -> RoomId -> UserId -> IO ()
ensureRoomMembership logger timeline mappingStore platform room user = do
  previous <- getMembership timeline user room
  alreadyJoined <- case previous of
    Nothing -> pure False
    Just previousId -> do
      previousEvent <- getEvent timeline previousId
      pure case fmap (.storedEvent.content) previousEvent of
        Just (JoinRoom _ _ _) -> True
        _ -> False
  unless alreadyJoined do
    let platformKey =
          PlatformKey
            platform
            "join"
            ( renderId (roomEventId room)
                <> "/"
                <> renderId (userEventId user)
                <> "/"
                <> maybe "none" renderId previous
            )
    result <- submitMapped logger timeline mappingStore Event {platformKey, content = JoinRoom user room previous}
    case result of
      Left err -> logError logger ("membership submit error: " <> Text.pack (show err))
      Right _ -> pure ()
 where
  renderId (EventId value) = value

submitMapped :: Timeline timeline => Logger -> timeline -> MappingStore -> Event -> IO (Either SubmitError EventId)
submitMapped logger timeline mappingStore event = do
  submitted <- submit timeline event
  case submitted of
    Right eventId -> record eventId
    Left (ConflictingPlatformKey _ eventId) -> record eventId
    Left err -> pure (Left err)
 where
  record eventId = do
    mappingStore.insertMapping event.platformKey eventId
    logObserved logger event.platformKey event.content eventId
    pure (Right eventId)

-- | Look up a timeline message that was sent by the timeline, for edits and
-- retractions that need to reuse its sender context.
lookupSendMessage :: Timeline timeline => timeline -> MessageId -> IO (Maybe SendMessageInfo)
lookupSendMessage timeline message = do
  original <- getEvent timeline (messageEventId message)
  pure case fmap (.storedEvent.content) original of
    Just (SendMessage info) -> Just info
    _ -> Nothing

-- | A platform target may have been mapped to any event in a message chain.
-- Normalise it to the chain root, which is the message identity.
resolveMessageId :: Timeline timeline => timeline -> EventId -> IO (Maybe MessageId)
resolveMessageId timeline target = getMessageRoot timeline target

-- | The first sighting of a platform user creates its timeline user. Profile
-- updates after that are submitted through 'syncUserProfile'.
ensureUserMapped :: Timeline timeline => Logger -> timeline -> MappingStore -> PlatformKey -> IO UserInfo -> IO (Either SubmitError UserId)
ensureUserMapped logger timeline mappingStore platformKey fetchInfo = do
  existing <- mappingStore.lookupTimelineId platformKey
  case existing of
    Just eventId -> pure (Right (UserId eventId))
    Nothing -> do
      info <- fetchInfo
      submitted <- submit timeline Event {platformKey, content = CreateUser info}
      case submitted of
        Right eventId -> do
          mappingStore.insertMapping platformKey eventId
          logObserved logger platformKey (CreateUser info) eventId
          pure (Right (UserId eventId))
        Left (ConflictingPlatformKey _ eventId) -> do
          let userId = UserId eventId
          mappingStore.insertMapping platformKey eventId
          syncUserProfile logger timeline mappingStore platformKey userId (pure (Just info))
          pure (Right userId)
        Left err -> pure (Left err)

-- | A modification is keyed by its predecessor and the resulting profile. The
-- predecessor alone is not enough: independent changes (for example a name and
-- an avatar) can share the same predecessor.
modifyUserKey :: PlatformKey -> UserId -> EventId -> UserInfo -> PlatformKey
modifyUserKey base user previous info =
  base
    { entityType = "user-modify"
    , key =
        renderEventId (userEventId user)
          <> "/"
          <> renderEventId previous
          <> "/"
          <> contentDigest (Text.pack (show info))
    }

-- | A room-scoped profile modification. The predecessor and the resulting
-- profile distinguish repeated transitions with the same platform key.
modifyUserPerRoomKey :: PlatformKey -> UserId -> RoomId -> EventId -> Maybe UserInfo -> PlatformKey
modifyUserPerRoomKey base user room previous info =
  base
    { entityType = "user-room-modify"
    , key =
        renderEventId (userEventId user)
          <> "/"
          <> renderEventId (roomEventId room)
          <> "/"
          <> renderEventId previous
          <> "/"
          <> contentDigest (Text.pack (show info))
    }

modifyRoomKey :: PlatformKey -> RoomId -> EventId -> RoomInfo -> PlatformKey
modifyRoomKey base room previous info =
  base
    { entityType = "room-modify"
    , key =
        renderEventId (roomEventId room)
          <> "/"
          <> renderEventId previous
          <> "/"
          <> contentDigest (Text.pack (show info))
    }

-- | The platform whose creation event owns a room's metadata. Shared timeline
-- rooms must not have several bridges overwriting each other's room profile.
roomOriginPlatform :: Timeline timeline => timeline -> RoomId -> IO (Maybe Text)
roomOriginPlatform timeline room = do
  stored <- getEvent timeline (roomEventId room)
  pure (stored >>= \event -> Just event.storedEvent.platformKey.platform)

-- | Apply a fetched profile only when the fetch succeeded. A failed avatar
-- download must not clear an existing avatar.
syncUserProfile ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  PlatformKey ->
  UserId ->
  IO (Maybe UserInfo) ->
  IO ()
syncUserProfile logger timeline mappingStore platformKey user fetchInfo = do
  fetched <- fetchInfo
  case fetched of
    Nothing -> pure ()
    Just info -> do
      current <- getUserInfo timeline user Nothing
      unless (current == Just info) do
        previous <- getUserProfileEvent timeline user
        case previous of
          Nothing -> logError logger ("cannot modify missing user " <> renderEventId (userEventId user))
          Just previousEventId -> do
            let content = ModifyUser user info previousEventId
                key = modifyUserKey platformKey user previousEventId info
            result <- submitMapped logger timeline mappingStore Event {platformKey = key, content}
            case result of
              Left err -> logError logger ("user modification submit error: " <> Text.pack (show err))
              Right _ -> pure ()

-- | Submit a room-scoped profile update under a stable platform key.
submitUserPerRoom ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  PlatformKey ->
  UserId ->
  RoomId ->
  Maybe UserInfo ->
  IO ()
submitUserPerRoom logger timeline mappingStore platformKey user room info = do
  current <- getRoomUserInfo timeline user room
  unless (current == info) do
    previous <- getUserProfileEvent timeline user
    case previous of
      Nothing -> logError logger ("cannot modify missing user " <> renderEventId (userEventId user))
      Just previousEventId -> do
        let content = ModifyUserPerRoom user room info previousEventId
        result <- submitMapped logger timeline mappingStore Event {platformKey, content}
        case result of
          Left err -> logError logger ("room user modification submit error: " <> Text.pack (show err))
          Right _ -> pure ()

-- | Submit a room-scoped profile update whose platform key is derived from the
-- predecessor and the new value. Used when the platform event has no stable
-- per-transition key of its own.
syncUserPerRoom ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  PlatformKey ->
  UserId ->
  RoomId ->
  Maybe UserInfo ->
  IO ()
syncUserPerRoom logger timeline mappingStore baseKey user room info = do
  current <- getRoomUserInfo timeline user room
  unless (current == info) do
    previous <- getUserProfileEvent timeline user
    case previous of
      Nothing -> logError logger ("cannot modify missing user " <> renderEventId (userEventId user))
      Just previousEventId -> do
        let content = ModifyUserPerRoom user room info previousEventId
            key = modifyUserPerRoomKey baseKey user room previousEventId info
        result <- submitMapped logger timeline mappingStore Event {platformKey = key, content}
        case result of
          Left err -> logError logger ("room user modification submit error: " <> Text.pack (show err))
          Right _ -> pure ()

syncRoomProfile ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  PlatformKey ->
  RoomId ->
  IO (Maybe RoomInfo) ->
  IO ()
syncRoomProfile logger timeline mappingStore platformKey room fetchInfo = do
  fetched <- fetchInfo
  case fetched of
    Nothing -> pure ()
    Just info -> do
      current <- getRoomInfo timeline room
      unless (current == Just info) do
        previous <- getRoomProfileEvent timeline room
        case previous of
          Nothing -> logError logger ("cannot modify missing room " <> renderEventId (roomEventId room))
          Just previousEventId -> do
            let content = ModifyRoom room info previousEventId
                key = modifyRoomKey platformKey room previousEventId info
            result <- submitMapped logger timeline mappingStore Event {platformKey = key, content}
            case result of
              Left err -> logError logger ("room modification submit error: " <> Text.pack (show err))
              Right _ -> pure ()

lookupBlobBytes :: Timeline timeline => timeline -> BlobId -> IO (Maybe ByteString)
lookupBlobBytes timeline blob = do
  stored <- getEvent timeline (blobEventId blob)
  pure case fmap (.storedEvent.content) stored of
    Just (CreateBlob bytes) -> Just bytes
    _ -> Nothing

-- | Avatars are best-effort image blobs. Any failure is logged and becomes
-- 'Nothing' so the caller can still create the user or room.
importAvatar ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  PlatformKey ->
  ByteString ->
  IO (Maybe BlobId)
importAvatar logger timeline mappingStore sourceKey bytes
  | ByteString.null bytes = do
      logDebug logger ("ignoring empty avatar " <> renderPlatformKey sourceKey)
      pure Nothing
  | ByteString.length bytes > maxAvatarBytes = do
      logDebug logger ("ignoring oversized avatar " <> renderPlatformKey sourceKey)
      pure Nothing
  | not ("image/" `Text.isPrefixOf` sniffMimeType bytes) = do
      logDebug logger ("ignoring non-image avatar " <> renderPlatformKey sourceKey)
      pure Nothing
  | otherwise = do
      result <- submitBlob logger timeline mappingStore sourceKey bytes
      case result of
        Right blob -> pure (Just blob)
        Left err -> do
          logWarn logger ("avatar submit error: " <> Text.pack (show err))
          pure Nothing

maxAvatarBytes :: Int
maxAvatarBytes = 8 * 1024 * 1024

-- | Keep reflecting the timeline from a persisted cursor, one batch at a time.
reflectForever :: Timeline timeline => timeline -> MappingStore -> Text -> (StoredEvent -> IO ()) -> IO ()
reflectForever timeline mappingStore stateKey reflectOne =
  forever do
    afterEventId <- fmap (fmap EventId) (mappingStore.lookupState stateKey)
    storedEvents <- getEventsAfter timeline afterEventId
    mapM_ reflectOne storedEvents
    case storedEvents of
      [] -> threadDelay 2000000
      _ -> mappingStore.setState stateKey (renderEventId ((last storedEvents).storedId))

submitMessageMapped :: Timeline timeline => Logger -> timeline -> MappingStore -> Event -> IO (Either SubmitError EventId)
submitMessageMapped logger timeline mappingStore event =
  case event.content of
    SendMessage info -> do
      submitted <- submitMapped logger timeline mappingStore event
      case submitted of
        Left (MissingReferences missing)
          | Just repliedMessage <- info.replyTo
          , messageEventId repliedMessage `elem` missing ->
              submitMapped logger timeline mappingStore event {content = SendMessage info {replyTo = Nothing}}
        other -> pure other
    _ -> submitMapped logger timeline mappingStore event

-- | Every event a bridge submits to the timeline and every event it reflects
-- to a platform is reported at info level, so relay traffic can be followed
-- without turning on debug logging for the HTTP traffic. Observed and
-- reflected events name the same two ends of the trip in the order the event
-- travelled.
logObserved :: Logger -> PlatformKey -> EventContent -> EventId -> IO ()
logObserved logger platformKey content eventId =
  logInfo logger
    ( "observed "
        <> eventKind content
        <> " "
        <> renderPlatformKey platformKey
        <> " -> "
        <> renderEventId eventId
    )

logReflected :: Logger -> StoredEvent -> PlatformKey -> IO ()
logReflected logger stored platformKey =
  logInfo logger
    ( "reflected "
        <> eventKind stored.storedEvent.content
        <> " "
        <> renderEventId stored.storedId
        <> " -> "
        <> renderPlatformKey platformKey
    )

eventKind :: EventContent -> Text
eventKind = \case
  CreateRoom _ -> "room creation"
  CreateUser _ -> "user creation"
  CreateBlob _ -> "blob creation"
  JoinRoom _ _ _ -> "room join"
  LeaveRoom _ _ _ -> "room leave"
  SendMessage _ -> "message"
  RetractMessage _ -> "retraction"
  EditMessage _ _ -> "edit"
  ModifyUser _ _ _ -> "user modification"
  ModifyUserPerRoom _ _ _ _ -> "room user modification"
  ModifyRoom _ _ _ -> "room modification"

-- | Blobs are timeline-owned: their event platform key is derived from the
-- content, while the bridge mapping records the platform-local source key.
submitBlob :: Timeline timeline => Logger -> timeline -> MappingStore -> PlatformKey -> ByteString -> IO (Either SubmitError BlobId)
submitBlob logger timeline mappingStore sourceKey bytes = do
  submitted <- submit timeline Event {platformKey = blobPlatformKey bytes, content = CreateBlob bytes}
  case submitted of
    Right eventId -> record eventId
    Left (ConflictingPlatformKey _ eventId) -> record eventId
    Left err -> pure (Left err)
 where
  record eventId = do
    mappingStore.insertMapping sourceKey eventId
    logObserved logger sourceKey (CreateBlob bytes) eventId
    pure (Right (BlobId eventId))

openMappingStore :: FilePath -> IO MappingStore
openMappingStore path = do
  connection <- open path
  initializeMappingStore connection
  -- The observer and the reflector of one bridge share the store, so the
  -- connection is guarded by an 'MVar' just like the timeline's.
  guarded <- newMVar connection
  let withDb :: (Connection -> IO a) -> IO a
      withDb = withMVar guarded
  pure
    MappingStore
      { lookupTimelineId = \platformKey -> withDb \connection' -> lookupTimelineIdSql connection' platformKey
      , lookupPlatformKeys = \eventId -> withDb \connection' -> lookupPlatformKeysSql connection' eventId
      , insertMapping = \platformKey eventId -> withDb \connection' -> insertMappingSql connection' platformKey eventId
      , lookupState = \key -> withDb \connection' -> lookupStateSql connection' key
      , setState = \key value -> withDb \connection' -> setStateSql connection' key value
      }

ensureInitialRooms ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  (InitialRoomMapping -> IO RoomInfo) ->
  [InitialRoomMapping] ->
  IO [InitialRoomMapping]
ensureInitialRooms logger timeline mappingStore fetchRoomInfo mappings = do
  logInfo logger ("ensuring " <> Text.pack (show (length mappings)) <> " initial room mappings")
  mapM ensureOne mappings
 where
  ensureOne mapping
    | mapping.enable = ensureInitialRoom logger timeline mappingStore fetchRoomInfo mapping
    | otherwise = pure mapping

ensureInitialRoom ::
  Timeline timeline =>
  Logger ->
  timeline ->
  MappingStore ->
  (InitialRoomMapping -> IO RoomInfo) ->
  InitialRoomMapping ->
  IO InitialRoomMapping
ensureInitialRoom logger timeline mappingStore fetchRoomInfo mapping = do
  existing <- mappingStore.lookupTimelineId mapping.platformKey
  case existing of
    Just mappedEventId -> do
      logDebug logger ("initial room already mapped " <> renderPlatformKey mapping.platformKey)
      refreshExisting mappedEventId
      pure mapping
    Nothing ->
      case mapping.timelineRoom of
        Just room -> do
          mappingStore.insertMapping mapping.platformKey (roomEventId room)
          logInfo logger ("mapped initial room " <> renderPlatformKey mapping.platformKey)
          refreshExisting (roomEventId room)
          pure mapping
        Nothing -> do
          roomInfo <- fetchRoomInfo mapping
          submitted <- submit timeline Event {platformKey = mapping.platformKey, content = CreateRoom roomInfo}
          case submitted of
            Right createdEventId -> do
              logInfo logger ("created initial room " <> renderPlatformKey mapping.platformKey)
              record createdEventId
            Left (ConflictingPlatformKey _ existingEventId) -> do
              logInfo logger ("recovered initial room mapping " <> renderPlatformKey mapping.platformKey)
              record existingEventId
            Left err -> ioError (userError ("could not create initial room mapping: " <> show err))
 where
  record mappedEventId = do
    mappingStore.insertMapping mapping.platformKey mappedEventId
    pure mapping {timelineRoom = Just (RoomId mappedEventId)}

  -- Only the platform that created the timeline room owns its metadata. A
  -- failed fetch must not clear an avatar already stored in the timeline.
  refreshExisting mappedEventId = do
    let roomId = RoomId mappedEventId
    origin <- roomOriginPlatform timeline roomId
    when (origin == Just mapping.platformKey.platform) do
      fetched <- try (fetchRoomInfo mapping) :: IO (Either SomeException RoomInfo)
      case fetched of
        Left err ->
          logWarn logger ("could not refresh initial room profile: " <> Text.pack (show err))
        Right (RoomInfo platformName platformDescription platformAvatar) -> do
          current <- getRoomInfo timeline roomId
          case current of
            Nothing -> pure ()
            Just (RoomInfo _ _ currentAvatar) -> do
              let merged =
                    RoomInfo
                      { name = platformName
                      , description = platformDescription
                      , avatar = platformAvatar <|> currentAvatar
                      }
              syncRoomProfile logger timeline mappingStore mapping.platformKey roomId (pure (Just merged))

renderPlatformKey :: PlatformKey -> Text
renderPlatformKey platformKey =
  platformKey.platform <> ":" <> platformKey.entityType <> ":" <> platformKey.key

-- | A bridge mapping is a single fact: this platform entity is that timeline
-- event. Rooms are part of the platform key because a platform key identifies
-- one platform-side entity on its own; one timeline event therefore maps to as
-- many platform keys as it was reflected into.
bridgeMappingsDdl :: Query
bridgeMappingsDdl =
  "CREATE TABLE IF NOT EXISTS bridge_mappings\
  \ (platform TEXT NOT NULL,\
  \  entity_type TEXT NOT NULL,\
  \  platform_key TEXT NOT NULL,\
  \  event_id TEXT NOT NULL,\
  \  UNIQUE (platform, entity_type, platform_key))"

bridgeMappingsIndexDdl :: Query
bridgeMappingsIndexDdl =
  "CREATE INDEX IF NOT EXISTS bridge_mappings_event_id_idx ON bridge_mappings (event_id)"

-- | Only opaque platform cursors and derived media handles belong here; the fact
-- that something was reflected is a @bridge_mappings@ row.
bridgeStateDdl :: Query
bridgeStateDdl =
  "CREATE TABLE IF NOT EXISTS bridge_state\
  \ (key TEXT PRIMARY KEY,\
  \  value TEXT NOT NULL)"

initializeMappingStore :: Connection -> IO ()
initializeMappingStore connection = do
  execute_ connection "PRAGMA journal_mode = WAL"
  execute_ connection "PRAGMA busy_timeout = 5000"
  execute_ connection bridgeStateDdl
  execute_ connection bridgeMappingsDdl
  execute_ connection bridgeMappingsIndexDdl

lookupTimelineIdSql :: Connection -> PlatformKey -> IO (Maybe EventId)
lookupTimelineIdSql connection platformKey = do
  rows <-
    query
      connection
      "SELECT event_id FROM bridge_mappings WHERE platform = ? AND entity_type = ? AND platform_key = ?"
      (platformKey.platform, platformKey.entityType, platformKey.key)
  case rows of
    Only eventIdText : _ ->
      case parsePersistedEventId eventIdText of
        Just mappedEventId -> pure (Just mappedEventId)
        Nothing -> ioError (userError ("invalid mapped event id: " <> Text.unpack eventIdText))
    [] -> pure Nothing

lookupPlatformKeysSql :: Connection -> EventId -> IO [PlatformKey]
lookupPlatformKeysSql connection mappedEventId = do
  rows <-
    query
      connection
      "SELECT platform, entity_type, platform_key FROM bridge_mappings WHERE event_id = ? ORDER BY platform, entity_type, platform_key"
      (Only (persistedEventId mappedEventId))
  pure (map toPlatformKey rows)

insertMappingSql :: Connection -> PlatformKey -> EventId -> IO ()
insertMappingSql connection platformKey mappedEventId =
  execute
    connection
    "INSERT OR IGNORE INTO bridge_mappings (platform, entity_type, platform_key, event_id) VALUES (?, ?, ?, ?)"
    (platformKey.platform, platformKey.entityType, platformKey.key, persistedEventId mappedEventId)

lookupStateSql :: Connection -> Text -> IO (Maybe Text)
lookupStateSql connection key = do
  rows <- query connection "SELECT value FROM bridge_state WHERE key = ?" (Only key)
  pure case rows of
    Only value : _ -> Just value
    [] -> Nothing

setStateSql :: Connection -> Text -> Text -> IO ()
setStateSql connection key value =
  execute
    connection
    "INSERT INTO bridge_state (key, value) VALUES (?, ?)\
    \ ON CONFLICT(key) DO UPDATE SET value = excluded.value"
    (key, value)

toPlatformKey :: (Text, Text, Text) -> PlatformKey
toPlatformKey (platform, entityType, key) =
  PlatformKey {platform, entityType, key}
