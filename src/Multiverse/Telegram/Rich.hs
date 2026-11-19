-- | Rich messages, i.e. the block-structured message format of the Telegram
-- bot API. The block model stays local to this module: parsing turns blocks
-- into the timeline 'Message' types and rendering turns them back into blocks,
-- so the rest of the program never sees a rich message.
module Multiverse.Telegram.Rich
  ( MediaRef (..)
  , RichBlock (..)
  , RichMedia (..)
  , RichMessage (..)
  , messageBlocks
  , parseRichMessage
  , renderRichMessage
  , richMessageMedia
  )
where

import Control.Applicative ((<|>))
import Control.Monad (join)
import Data.Aeson
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Aeson.Types (Parser, parseEither, parseMaybe)
import Data.Foldable (toList)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes)
import Data.Text (Text)
import Data.Text qualified as Text
import Multiverse.Event
import Multiverse.Types (BlobId, BlobType (..))

-- | A platform resource inside a rich message. @file_unique_id@ is stable
-- across bots, so it identifies the resource when the server provides it.
data MediaRef = MediaRef
  { fileId :: Maybe Text
  , fileUniqueId :: Maybe Text
  }
  deriving (Eq, Show)

data RichMedia = RichMedia
  { kind :: BlobType
  , media :: MediaRef
  , caption :: Maybe InlineText
  }
  deriving (Eq, Show)

data RichBlock
  = RichParagraph InlineText
  | RichHeading Int InlineText
  | RichPre (Maybe Text) Text
  | RichList [[RichBlock]]
  | RichQuote [RichBlock]
  | RichDetails InlineText [RichBlock]
  | RichTable [[Text]]
  | RichDivider
  | RichExpression Text
  | RichMediaBlock RichMedia
  deriving (Eq, Show)

newtype RichMessage = RichMessage {blocks :: [RichBlock]}
  deriving (Eq, Show)

-- | Parse the @rich_message@ field of a bot API message. An unparsable message
-- is reported instead of dropped silently, because the observer has no other
-- representation of it.
parseRichMessage :: Value -> Either Text RichMessage
parseRichMessage value = either (Left . Text.pack) Right (parseEither parseJSON value)

instance FromJSON RichMessage where
  parseJSON = withObject "RichMessage" \obj ->
    RichMessage <$> (obj .: "blocks" >>= parseRichBlocks)

-- Parsing: bot API blocks -> timeline message ---------------------------------

parseRichBlocks :: [Value] -> Parser [RichBlock]
parseRichBlocks = fmap concat . traverse parseRichBlock

-- Unknown blocks (anchors, buttons, maps, drafts) are dropped: they have no
-- timeline counterpart and hold nothing another platform could render.
parseRichBlock :: Value -> Parser [RichBlock]
parseRichBlock = withObject "RichBlock" \obj -> do
  blockType <- obj .: "type"
  case (blockType :: Text) of
    "paragraph" -> single . RichParagraph <$> inlineField obj "text"
    "footer" -> single . RichParagraph <$> inlineField obj "text"
    "heading" -> single <$> (RichHeading <$> obj .: "size" <*> inlineField obj "text")
    "pre" -> do
      language <- obj .:? "language"
      text <- inlineToText <$> inlineField obj "text"
      pure [RichPre language text]
    "list" -> single . RichList <$> listItems obj
    "blockquote" -> single . RichQuote <$> (obj .: "blocks" >>= parseRichBlocks)
    "expandable_blockquote" -> single . RichQuote . single . RichParagraph <$> inlineField obj "text"
    "pullquote" -> single . RichQuote . single . RichParagraph <$> inlineField obj "text"
    "details" -> do
      summary <- inlineField obj "summary"
      content <- obj .: "blocks" >>= parseRichBlocks
      pure [RichDetails summary content]
    "table" -> single . RichTable <$> tableRows obj
    "divider" -> pure [RichDivider]
    "mathematical_expression" -> single . RichExpression <$> obj .: "expression"
    "collage" -> mediaGroup obj
    "slideshow" -> mediaGroup obj
    "photo" -> mediaBlock ImageBlob "photo" obj
    "video" -> mediaBlock VideoBlob "video" obj
    "animation" -> mediaBlock VideoBlob "animation" obj
    "audio" -> mediaBlock AudioBlob "audio" obj
    "voice_note" -> mediaBlock AudioBlob "voice_note" obj
    "document" -> mediaBlock FileBlob "document" obj
    _ -> pure []
 where
  single value = [value]

inlineField :: Object -> Key -> Parser InlineText
inlineField obj key =
  maybe (plainInline "") parseRichInline <$> (obj .:? key :: Parser (Maybe Value))

listItems :: Object -> Parser [[RichBlock]]
listItems obj = do
  items <- obj .: "items" :: Parser [Value]
  traverse (withObject "RichBlockListItem" \item -> item .: "blocks" >>= parseRichBlocks) items

-- The timeline has no table, so each row becomes a line of cells.
tableRows :: Object -> Parser [[Text]]
tableRows obj = do
  rows <- obj .: "cells" :: Parser [[Value]]
  pure (map (map cellText) rows)
 where
  cellText cell = maybe "" (inlineToText . parseRichInline) (join (parseMaybe cellTextValue cell))
  cellTextValue value = withObject "RichBlockTableCell" (\cell -> cell .:? "text") value :: Parser (Maybe Value)

mediaBlock :: BlobType -> Key -> Object -> Parser [RichBlock]
mediaBlock mediaKind key obj = do
  raw <- obj .: key :: Parser Value
  captionInline <- captionField obj
  pure [RichMediaBlock RichMedia {kind = mediaKind, media = mediaRef raw, caption = captionInline}]

-- Collages and slideshows are media collections: keep the media, drop the
-- grouping, which the timeline has no counterpart for.
mediaGroup :: Object -> Parser [RichBlock]
mediaGroup obj = do
  content <- obj .: "blocks" >>= parseRichBlocks
  captionInline <- captionField obj
  let medias = [rich | RichMediaBlock rich <- content]
  pure (case medias of
    [] -> []
    firstMedia : rest ->
      RichMediaBlock firstMedia {caption = captionInline <|> firstMedia.caption} : map RichMediaBlock rest)

captionField :: Object -> Parser (Maybe InlineText)
captionField obj = do
  raw <- obj .:? "caption" :: Parser (Maybe Value)
  pure (parseRichInline <$> (raw >>= captionText))
 where
  captionText value = join (parseMaybe captionValue value)
  captionValue value = withObject "RichBlockCaption" (\caption -> caption .:? "text") value :: Parser (Maybe Value)

mediaRef :: Value -> MediaRef
mediaRef = \case
  -- A photo block lists its sizes, largest last.
  Array sizes -> maybe emptyRef mediaRef (lastMaybe (toList sizes))
  Object obj ->
    MediaRef
      { fileId = valueText =<< KeyMap.lookup "file_id" obj
      , fileUniqueId = valueText =<< KeyMap.lookup "file_unique_id" obj
      }
  _ -> emptyRef
 where
  emptyRef = MediaRef {fileId = Nothing, fileUniqueId = Nothing}
  lastMaybe [] = Nothing
  lastMaybe values = Just (last values)
  valueText = \case
    String text -> Just text
    _ -> Nothing

-- | Rich text is a union of a plain string, an array of rich texts and tagged
-- objects. Tags the timeline cannot represent are kept as their HTML form in a
-- plain part, so the information survives the timeline instead of vanishing.
parseRichInline :: Value -> InlineText
parseRichInline = \case
  String text -> plainInline text
  Array values -> inlineFromParts (concatMap (toList . parseRichInline) (toList values))
  Object obj -> parseRichInlineObject obj
  _ -> plainInline ""

inlineFromParts :: [InlineTextPart] -> InlineText
inlineFromParts parts = case NonEmpty.nonEmpty parts of
  Just inline -> inline
  Nothing -> plainInline ""

parseRichInlineObject :: Object -> InlineText
parseRichInlineObject obj =
  case textAt "type" of
    "bold" -> wrap Bold
    "italic" -> wrap Italic
    "code" -> wrap InlineQuote
    "underline" -> htmlTag "u"
    "strikethrough" -> htmlTag "s"
    "spoiler" -> htmlTag "tg-spoiler"
    "marked" -> htmlTag "mark"
    "subscript" -> htmlTag "sub"
    "superscript" -> htmlTag "sup"
    "url" -> linked (textAt "url")
    "email_address" -> linked ("mailto:" <> textAt "email_address")
    "phone_number" -> linked ("tel:" <> textAt "phone_number")
    "text_mention" -> htmlWrapped ("<a href=\"tg://user?id=" <> mentionedUserId <> "\">") "</a>"
    "date_time" ->
      htmlWrapped
        ("<tg-time unix=\"" <> textAt "unix_time" <> "\" format=\"" <> textAt "date_time_format" <> "\">")
        "</tg-time>"
    "custom_emoji" ->
      plainInline
        ("<tg-emoji emoji-id=\"" <> textAt "custom_emoji_id" <> "\">" <> textAt "alternative_text" <> "</tg-emoji>")
    "mathematical_expression" -> plainInline ("<tg-math>" <> textAt "expression" <> "</tg-math>")
    _ -> plainInline (inlineToText inner)
 where
  inner = maybe (plainInline "") parseRichInline (KeyMap.lookup "text" obj)
  textAt key = maybe "" scalarText (KeyMap.lookup key obj)
  wrap constructor = constructor inner :| []
  htmlTag name = htmlWrapped ("<" <> name <> ">") ("</" <> name <> ">")
  htmlWrapped open close = inlineFromParts (Plain open : toList inner <> [Plain close])
  linked url = Link inner url :| []
  mentionedUserId = case KeyMap.lookup "user" obj of
    Just (Object user) -> maybe "" scalarText (KeyMap.lookup "id" user)
    _ -> ""

scalarText :: Value -> Text
scalarText = \case
  String text -> text
  Number number -> Text.pack (show (truncate number :: Integer))
  Bool True -> "true"
  Bool False -> "false"
  _ -> ""

-- | The unformatted text of a rich text, for blocks that hold text only.
inlineToText :: InlineText -> Text
inlineToText = Text.concat . map partText . toList
 where
  partText = \case
    Plain text -> text
    Bold text -> inlineToText text
    Italic text -> inlineToText text
    Link text _ -> inlineToText text
    Mention text _ -> inlineToText text
    InlineQuote text -> inlineToText text

-- | Turn blocks into timeline message parts. Media resolution is a callback
-- because downloading and submitting blobs is the bridge's business.
renderRichMessage :: (BlobType -> MediaRef -> IO BlobId) -> RichMessage -> IO (Maybe Message)
renderRichMessage resolve rich = messageFromBlocks resolve rich.blocks

messageFromBlocks :: (BlobType -> MediaRef -> IO BlobId) -> [RichBlock] -> IO (Maybe Message)
messageFromBlocks resolve blocks_ =
  fmap NonEmpty.nonEmpty (fmap concat (traverse (partOf resolve) blocks_))

partOf :: (BlobType -> MediaRef -> IO BlobId) -> RichBlock -> IO [MessagePart]
partOf resolve = \case
  RichParagraph inline -> pure [Text inline]
  RichHeading level inline -> pure [Heading level inline]
  RichPre _ text -> pure [Text (InlineQuote (plainInline text) :| [])]
  RichList items -> do
    messages <- traverse (messageFromBlocks resolve) items
    pure (maybe [] (\nested -> [List nested]) (NonEmpty.nonEmpty (catMaybes messages)))
  RichQuote content -> do
    nested <- messageFromBlocks resolve content
    pure (maybe [] (\message -> [BlockQuote message]) nested)
  RichDetails summary content -> do
    nested <- messageFromBlocks resolve content
    pure
      (maybe
        []
        (\message -> [BlockQuote (Text summary :| toList message)])
        nested)
  RichTable rows ->
    pure [Text (plainInline (Text.intercalate "\n" (map (Text.intercalate " | ") rows)))]
  RichDivider -> pure [Text (plainInline "———")]
  RichExpression expression -> pure [Text (InlineQuote (plainInline expression) :| [])]
  RichMediaBlock rich -> do
    blob <- resolve rich.kind rich.media
    pure ([Blob rich.kind blob] <> maybe [] (\captionInline -> [Text captionInline]) rich.caption)

-- | The media of a message in block order, used to learn the file ids the bot
-- API returned for freshly uploaded files.
richMessageMedia :: RichMessage -> [MediaRef]
richMessageMedia = concatMap blockMedia . (.blocks)
 where
  blockMedia = \case
    RichMediaBlock rich -> [rich.media]
    RichList items -> concatMap (concatMap blockMedia) items
    RichQuote content -> concatMap blockMedia content
    RichDetails _ content -> concatMap blockMedia content
    _ -> []

-- Rendering: timeline message -> bot API blocks -------------------------------

-- | Render a message as rich blocks. Blobs are addressed through the given
-- function, which is how the bridge chooses between a reused file id and an
-- upload.
messageBlocks :: (BlobType -> BlobId -> Value) -> Message -> [Value]
messageBlocks blobBlock = concatMap partBlocks . toList
 where
  partBlocks = \case
    Text inline -> [paragraph inline]
    Emote inline -> [paragraph (Italic inline :| [])]
    Heading level inline ->
      [ object
          [ "type" .= ("heading" :: Text)
          , "size" .= max 1 (min 6 level)
          , "text" .= inlineValue inline
          ]
      ]
    List items ->
      [ object
          [ "type" .= ("list" :: Text)
          , "items" .= map (\item -> object ["blocks" .= messageBlocks blobBlock item]) (toList items)
          ]
      ]
    BlockQuote inline ->
      [object ["type" .= ("blockquote" :: Text), "blocks" .= messageBlocks blobBlock inline]]
    Blob kind blob -> [blobBlock kind blob]
  paragraph inline =
    object ["type" .= ("paragraph" :: Text), "text" .= inlineValue inline]

inlineValue :: InlineText -> Value
inlineValue inline = case toList inline of
  [part] -> partValue part
  parts -> toJSON (map partValue parts)

partValue :: InlineTextPart -> Value
partValue = \case
  Plain text -> String text
  Bold inline -> tagged "bold" inline
  Italic inline -> tagged "italic" inline
  InlineQuote inline -> tagged "code" inline
  Link inline url ->
    object
      [ "type" .= ("url" :: Text)
      , "text" .= inlineValue inline
      , "url" .= url
      ]
  -- A timeline mention has no platform-independent target, so it keeps its
  -- text; the bridges that know the platform user can relink it.
  Mention inline _ -> inlineValue inline
 where
  tagged name inline =
    object ["type" .= (name :: Text), "text" .= inlineValue inline]
