{ mkDerivation, aeson, base, base64-bytestring, bytestring
, containers, cryptohash-sha256, directory, http-client, http-client-tls
, http-types, katip, lib, sqlite-simple, tagsoup, text, tomland
, websockets
}:
mkDerivation {
  pname = "multiverse";
  version = "0.1.0.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    aeson base base64-bytestring bytestring containers cryptohash-sha256
    directory http-client http-client-tls http-types katip sqlite-simple
    tagsoup text tomland websockets
  ];
  executableHaskellDepends = [ base ];
  license = lib.meta.getLicenseFromSpdxId "AGPL-3.0-or-later";
  mainProgram = "multiverse";
}
