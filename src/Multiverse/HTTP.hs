module Multiverse.HTTP
  ( HttpLogOptions (..)
  , defaultHttpLogOptions
  , loggedHttp
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (try)
import Data.ByteString.Char8 qualified as ByteString.Char8
import Data.ByteString.Lazy qualified as LByteString
import Data.Char (toLower)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.Encoding.Error (lenientDecode)
import Network.HTTP.Client
import Network.HTTP.Types (hContentType, statusCode)
import Multiverse.Log

data HttpLogOptions = HttpLogOptions
  { redactUrl :: Text -> Text
  , requestSuffix :: Text
  }

defaultHttpLogOptions :: HttpLogOptions
defaultHttpLogOptions =
  HttpLogOptions
    { redactUrl = id
    , requestSuffix = ""
    }

loggedHttp :: Logger -> HttpLogOptions -> Request -> Manager -> IO (Response LByteString.ByteString)
loggedHttp logger options request manager = do
  logDebug logger ("http request " <> requestSummary options request)
  response <- httpLbsWithBackoff logger request manager
  logDebug logger ("http response " <> responseSummary options request response)
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

requestSummary :: HttpLogOptions -> Request -> Text
requestSummary options request =
  Text.pack (ByteString.Char8.unpack request.method)
    <> " "
    <> requestUrl options request
    <> " request_body="
    <> requestBodyPreview request
    <> options.requestSuffix

responseSummary :: HttpLogOptions -> Request -> Response LByteString.ByteString -> Text
responseSummary options request response =
  Text.pack (ByteString.Char8.unpack request.method)
    <> " "
    <> requestUrl options request
    <> " status="
    <> Text.pack (show (statusCode response.responseStatus))
    <> " response_body="
    <> bodyPreview (lookup hContentType response.responseHeaders) response.responseBody

requestBodyPreview :: Request -> Text
requestBodyPreview request =
  case request.requestBody of
    RequestBodyLBS body -> bodyPreview contentType body
    RequestBodyBS body -> bodyPreview contentType (LByteString.fromStrict body)
    RequestBodyBuilder size _ -> "builder:" <> Text.pack (show size) <> " bytes"
    RequestBodyStream size _ -> "stream:" <> Text.pack (show size) <> " bytes"
    RequestBodyStreamChunked _ -> "chunked"
    RequestBodyIO _ -> "io"
 where
  contentType = lookup hContentType request.requestHeaders

-- | Never render binary payloads. Textual payloads are truncated and any
-- embedded base64 blob data (for example OneBot's @base64://@ file fields)
-- is redacted before it reaches the logs.
bodyPreview :: Maybe ByteString.Char8.ByteString -> LByteString.ByteString -> Text
bodyPreview contentType body
  | LByteString.null body = "empty"
  | not (isTextualContentType contentType) = binarySummary body
  | looksBinary text = binarySummary body
  | otherwise =
      redactBlobData text
        <> if LByteString.length body > previewLimit then "...<truncated>" else ""
 where
  text = Text.decodeUtf8With lenientDecode (LByteString.toStrict (LByteString.take previewLimit body))

previewLimit :: Int64
previewLimit = 512

binarySummary :: LByteString.ByteString -> Text
binarySummary body = "binary:" <> Text.pack (show (LByteString.length body)) <> " bytes"

isTextualContentType :: Maybe ByteString.Char8.ByteString -> Bool
isTextualContentType Nothing = False
isTextualContentType (Just raw) =
  let contentType = ByteString.Char8.map toLower raw
   in "text/" `ByteString.Char8.isPrefixOf` contentType
        || "application/json" `ByteString.Char8.isInfixOf` contentType
        || "application/xml" `ByteString.Char8.isInfixOf` contentType
        || "application/x-www-form-urlencoded" `ByteString.Char8.isPrefixOf` contentType

looksBinary :: Text -> Bool
looksBinary = Text.any (== '\NUL')

redactBlobData :: Text -> Text
redactBlobData text = redactAfter "base64://" (redactAfter "base64," text)
 where
  redactAfter marker value =
    let (before, rest) = Text.breakOn marker value
     in if Text.null rest
          then value
          else before <> marker <> "<redacted>"

requestUrl :: HttpLogOptions -> Request -> Text
requestUrl options request =
  options.redactUrl
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
