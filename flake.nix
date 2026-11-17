{
  description = "";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/b4fd65b198c599cbe814fcb9f42d25d021595ec9";

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
