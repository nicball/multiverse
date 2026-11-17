module Multiverse.Bridge
  ( Bridge (..)
  , BridgeContext (..)
  , InitialRoomMapping (..)
  , enabledRoom
  , MappingStore (..)
  , RenderedMessage (..)
  , hasPlatformMapping
  , renderRelayedMessage
  , renderMessageBody
  , renderNickChange
  , ensureRoomMembership
  , submitMapped
  , submitMessageMapped
  , openMappingStore
  , ensureInitialRooms
  , ensureInitialRoomsWith
  , textBody
  , messageBlobs
  , hasTextContent
  , sniffExtension
  , sniffMimeType
  , isAnimatedImage
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty, toList)
import Data.Text (Text)
import Data.Text qualified as Text
import Database.SQLite.Simple
import Text.Read (readMaybe)
import Multiverse.Event hiding (eventId)
import Multiverse.Log
import Multiverse.Timeline
import Multiverse.Types

data Bridge timeline = Bridge
  { bridgeName :: Text
  , observe :: BridgeContext timeline -> IO ()
  , reflect :: BridgeContext timeline -> IO ()
  }

data BridgeContext timeline = BridgeContext
  { timeline :: timeline
  , mappingStore :: MappingStore
  , logger :: Logger
  }

data MappingStore = MappingStore
  { lookupTimelineId :: PlatformKey -> IO (Maybe EventId)
  , lookupPlatformKeys :: EventId -> IO [PlatformKey]
  , insertMapping :: PlatformKey -> EventId -> IO ()
  , lookupState :: Text -> IO (Maybe Text)
  , setState :: Text -> Text -> IO ()
  , closeMappingStore :: IO ()
  }

data InitialRoomMapping = InitialRoomMapping
  { platformKey :: PlatformKey
  , timelineRoom :: Maybe RoomId
  , enable :: Bool
  }
  deriving (Eq, Show)

enabledRoom :: [InitialRoomMapping] -> PlatformKey -> Bool
enabledRoom mappings key = any (\mapping -> mapping.enable && mapping.platformKey == key) mappings

data RenderedMessage = RenderedMessage
  { plainText :: Text
  , htmlText :: Text
  }

hasPlatformMapping :: MappingStore -> Text -> EventId -> IO Bool
hasPlatformMapping store platform eventId = any ((== platform) . (.platform)) <$> store.lookupPlatformKeys eventId

renderRelayedMessage :: Timeline timeline => BridgeContext timeline -> SendMessageInfo -> IO RenderedMessage
renderRelayedMessage context info = do
  user <- getUserInfo context.timeline info.sender (Just info.room)
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

renderNickChange :: Timeline timeline => BridgeContext timeline -> UserId -> Text -> IO RenderedMessage
renderNickChange context user nick = do
  userInfo <- getUserInfo context.timeline user Nothing
  let previous = maybe "unknown" (.name) userInfo
  pure
    RenderedMessage
      { plainText = "* " <> previous <> " is now known as " <> nick
      , htmlText = "<em>" <> htmlEscape previous <> " is now known as " <> htmlEscape nick <> "</em>"
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
          , htmlText = "<a href=\"" <> htmlAttributeEscape url <> "\">" <> rendered.htmlText <> "</a>"
          }
  Mention inline _ -> renderInline inline
  InlineQuote inline ->
    let rendered = renderInline inline
     in RenderedMessage ("\"" <> rendered.plainText <> "\"") ("<code>" <> rendered.htmlText <> "</code>")
  Plain text -> RenderedMessage text (htmlEscape text)

htmlEscape :: Text -> Text
htmlEscape =
  Text.concatMap \case
    '&' -> "&amp;"
    '<' -> "&lt;"
    '>' -> "&gt;"
    '"' -> "&quot;"
    '\'' -> "&#39;"
    char -> Text.singleton char

htmlAttributeEscape :: Text -> Text
htmlAttributeEscape = htmlEscape

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

plainMessage :: Text -> Message
plainMessage text = Text (Plain text :| []) :| []

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
-- room also establishes their membership. The key is derived from the referenced
-- events, so repeated sightings dedupe to the same event.
ensureRoomMembership :: Timeline timeline => BridgeContext timeline -> Text -> RoomId -> UserId -> IO ()
ensureRoomMembership context platform room user = do
  let platformKey =
        PlatformKey
          platform
          "join"
          (renderId (roomEventId room) <> "/" <> renderId (userEventId user))
  result <- submitMapped context Event {platformKey, content = JoinRoom user room}
  case result of
    Left err -> logError context.logger ("membership submit error: " <> Text.pack (show err))
    Right _ -> pure ()
 where
  renderId (EventId value) = value

submitMapped :: Timeline timeline => BridgeContext timeline -> Event -> IO (Either SubmitError EventId)
submitMapped context event = do
  submitted <- submit context.timeline event
  case submitted of
    Right eventId -> record eventId
    Left (ConflictingPlatformKey _ eventId) -> record eventId
    Left err -> pure (Left err)
 where
  record eventId = do
    context.mappingStore.insertMapping event.platformKey eventId
    pure (Right eventId)

submitMessageMapped :: Timeline timeline => BridgeContext timeline -> Event -> IO (Either SubmitError EventId)
submitMessageMapped context event =
  case event.content of
    SendMessage info -> do
      submitted <- submitMapped context event
      case submitted of
        Left (MissingReferences missing)
          | Just repliedMessage <- info.replyTo
          , messageEventId repliedMessage `elem` missing ->
              submitMapped context event {content = SendMessage info {replyTo = Nothing}}
        other -> pure other
    _ -> submitMapped context event

openMappingStore :: FilePath -> IO MappingStore
openMappingStore path = do
  connection <- open path
  migrateMappingStore connection
  pure
    MappingStore
      { lookupTimelineId = lookupTimelineIdSql connection
      , lookupPlatformKeys = lookupPlatformKeysSql connection
      , insertMapping = insertMappingSql connection
      , lookupState = lookupStateSql connection
      , setState = setStateSql connection
      , closeMappingStore = close connection
      }

ensureInitialRooms :: Timeline timeline => [InitialRoomMapping] -> BridgeContext timeline -> IO ()
ensureInitialRooms mappings context =
  ensureInitialRoomsWith
    mappings
    context
    (\mapping -> pure RoomInfo {name = mapping.platformKey.key, description = "", avatar = Nothing})
    (\_ _ -> pure ())

ensureInitialRoomsWith ::
  Timeline timeline =>
  [InitialRoomMapping] ->
  BridgeContext timeline ->
  (InitialRoomMapping -> IO RoomInfo) ->
  (InitialRoomMapping -> RoomId -> IO ()) ->
  IO ()
ensureInitialRoomsWith mappings context fetchRoomInfo writeBack = do
  logInfo context.logger ("ensuring " <> Text.pack (show (length mappings)) <> " initial room mappings")
  mapM_ (ensureInitialRoom context fetchRoomInfo writeBack) (filter (.enable) mappings)

ensureInitialRoom ::
  Timeline timeline =>
  BridgeContext timeline ->
  (InitialRoomMapping -> IO RoomInfo) ->
  (InitialRoomMapping -> RoomId -> IO ()) ->
  InitialRoomMapping ->
  IO ()
ensureInitialRoom context fetchRoomInfo writeBack mapping = do
  existing <- context.mappingStore.lookupTimelineId mapping.platformKey
  case existing of
    Just _ ->
      logDebug context.logger ("initial room already mapped " <> renderPlatformKey mapping.platformKey)
    Nothing ->
      case mapping.timelineRoom of
        Just room -> do
          context.mappingStore.insertMapping mapping.platformKey (roomEventId room)
          logInfo context.logger ("mapped initial room " <> renderPlatformKey mapping.platformKey)
        Nothing -> do
          roomInfo <- fetchRoomInfo mapping
          submitted <- submit context.timeline Event {platformKey = mapping.platformKey, content = CreateRoom roomInfo}
          case submitted of
            Right createdEventId -> do
              recordInitialRoom context writeBack mapping createdEventId
              logInfo context.logger ("created initial room " <> renderPlatformKey mapping.platformKey)
            Left (ConflictingPlatformKey _ existingEventId) -> do
              recordInitialRoom context writeBack mapping existingEventId
              logInfo context.logger ("recovered initial room mapping " <> renderPlatformKey mapping.platformKey)
            Left err -> ioError (userError ("could not create initial room mapping: " <> show err))

recordInitialRoom ::
  BridgeContext timeline ->
  (InitialRoomMapping -> RoomId -> IO ()) ->
  InitialRoomMapping ->
  EventId ->
  IO ()
recordInitialRoom context writeBack mapping mappedEventId = do
  let room = RoomId mappedEventId
  context.mappingStore.insertMapping mapping.platformKey mappedEventId
  writeBack mapping room

renderPlatformKey :: PlatformKey -> Text
renderPlatformKey platformKey =
  platformKey.platform <> ":" <> platformKey.entityType <> ":" <> platformKey.key

migrateMappingStore :: Connection -> IO ()
migrateMappingStore connection = do
  execute_
    connection
    "CREATE TABLE IF NOT EXISTS bridge_mappings\
    \ (platform TEXT NOT NULL,\
    \  entity_type TEXT NOT NULL,\
    \  platform_key TEXT NOT NULL,\
    \  event_id TEXT NOT NULL,\
    \  UNIQUE (platform, entity_type, platform_key),\
    \  UNIQUE (event_id, platform, entity_type, platform_key))"
  execute_
    connection
    "CREATE INDEX IF NOT EXISTS bridge_mappings_event_id_idx ON bridge_mappings (event_id)"
  execute_
    connection
    "CREATE TABLE IF NOT EXISTS bridge_state\
    \ (key TEXT PRIMARY KEY,\
    \  value TEXT NOT NULL)"

lookupTimelineIdSql :: Connection -> PlatformKey -> IO (Maybe EventId)
lookupTimelineIdSql connection platformKey = do
  rows <-
    query
      connection
      "SELECT event_id FROM bridge_mappings WHERE platform = ? AND entity_type = ? AND platform_key = ?"
      (platformKey.platform, platformKey.entityType, platformKey.key)
  case rows of
    Only eventIdText : _ ->
      case readEventId eventIdText of
        Just mappedEventId -> pure (Just mappedEventId)
        Nothing -> ioError (userError ("invalid mapped event id: " <> Text.unpack eventIdText))
    [] -> pure Nothing

lookupPlatformKeysSql :: Connection -> EventId -> IO [PlatformKey]
lookupPlatformKeysSql connection mappedEventId = do
  rows <-
    query
      connection
      "SELECT platform, entity_type, platform_key FROM bridge_mappings WHERE event_id = ? ORDER BY platform, entity_type, platform_key"
      (Only (renderEventId mappedEventId))
  pure (map toPlatformKey rows)

insertMappingSql :: Connection -> PlatformKey -> EventId -> IO ()
insertMappingSql connection platformKey mappedEventId =
  execute
    connection
    "INSERT OR IGNORE INTO bridge_mappings (platform, entity_type, platform_key, event_id) VALUES (?, ?, ?, ?)"
    (platformKey.platform, platformKey.entityType, platformKey.key, renderEventId mappedEventId)

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

renderEventId :: EventId -> Text
renderEventId = Text.pack . show

readEventId :: Text -> Maybe EventId
readEventId = readMaybe . Text.unpack
