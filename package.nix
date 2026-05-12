{ mkDerivation, base, lib }:
mkDerivation {
  pname = "multiverse";
  version = "0.1.0.0";
  src = ./.;
  isLibrary = false;
  isExecutable = true;
  executableHaskellDepends = [ base ];
  license = lib.licensesSpdx."AGPL-3.0-or-later";
  mainProgram = "multiverse";
}
