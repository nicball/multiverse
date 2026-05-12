{
  description = "";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/15f4ee454b1dce334612fa6843b3e05cf546efab";

  outputs = { self, nixpkgs }: {

    packages.x86_64-linux.multiverse =
      let hspkgs = nixpkgs.legacyPackages.x86_64-linux.haskellPackages; in
      hspkgs.callPackage ./package.nix {};

    packages.x86_64-linux.default = self.packages.x86_64-linux.multiverse;

    devShells.x86_64-linux.default =
      with nixpkgs.legacyPackages.x86_64-linux;
      haskellPackages.shellFor {
        packages = _: [ self.packages.x86_64-linux.multiverse ];
        nativeBuildInputs = [ haskell-language-server cabal2nix cabal-install ];
      };

  };
}
