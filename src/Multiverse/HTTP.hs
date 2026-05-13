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
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.Encoding.Error (lenientDecode)
import Network.HTTP.Client
import Network.HTTP.Types (statusCode)
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
    <> requestBodyPreview request.requestBody
    <> options.requestSuffix

responseSummary :: HttpLogOptions -> Request -> Response LByteString.ByteString -> Text
responseSummary options request response =
  Text.pack (ByteString.Char8.unpack request.method)
    <> " "
    <> requestUrl options request
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
