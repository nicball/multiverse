{ mkDerivation, aeson, base, bytestring, cryptohash-sha256
, directory, http-client, http-client-tls, http-types, katip, lib
, sqlite-simple, tagsoup, text, tomland
}:
mkDerivation {
  pname = "multiverse";
  version = "0.1.0.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    aeson base bytestring cryptohash-sha256 directory http-client
    http-client-tls http-types katip sqlite-simple tagsoup text tomland
  ];
  executableHaskellDepends = [ base ];
  license = lib.licensesSpdx."AGPL-3.0-or-later";
  mainProgram = "multiverse";
}
