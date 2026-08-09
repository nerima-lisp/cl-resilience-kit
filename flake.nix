{
  description = "Composable, dependency-neutral resilience primitives for Common Lisp";

  inputs = {
    # Use the tested NixOS channel rather than an independently moving
    # nixpkgs-unstable input.
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    # cl-nix-forge builds ASDF systems and derives the package, test check,
    # development shell, formatter, and documentation outputs from one call.
    cl-nix-forge = {
      url = "github:nerima-lisp/cl-nix-forge/v0.5.0";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    # Tests only. Keeping cl-weave out of lispDependencies means a consumer of
    # the library does not fetch the test framework.
    cl-weave = {
      url = "github:nerima-lisp/cl-weave/v1.3.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    # Main-system dependencies. This project uses the boundary package for
    # clocks, random sources, and sleepers, and date-kit for executor timing.
    cl-boundary-kit = {
      url = "github:nerima-lisp/cl-boundary-kit/v2.3.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-weave.follows = "cl-weave";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    cl-concurrent-kit = {
      url = "github:nerima-lisp/cl-concurrent-kit/v0.6.1";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-boundary-kit.follows = "cl-boundary-kit";
      inputs.cl-date-kit.follows = "cl-date-kit";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    # cl-concurrent-kit's flake exposes cl-date-kit as a real input. Follow the
    # same node here so the dependency graph has one date-kit version.
    cl-date-kit = {
      url = "github:nerima-lisp/cl-date-kit/v1.0.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      cl-nix-forge,
      cl-weave,
      cl-boundary-kit,
      cl-concurrent-kit,
      cl-date-kit,
      treefmt-nix,
      ...
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-darwin"
      ];
      testTimeoutSeconds = 120;
      coverageTimeoutSeconds = 300;
      terminationGraceSeconds = 10;
      cl = cl-nix-forge.lib.${nixpkgs.lib.head systems};
      meta = {
        description = "Composable, dependency-neutral resilience primitives for Common Lisp";
        homepage = "https://github.com/nerima-lisp/cl-resilience-kit";
        license = nixpkgs.lib.licenses.mit;
      };
    in
    cl.mkPackageFlake {
      inherit
        self
        nixpkgs
        systems
        meta
        ;
      pname = "cl-resilience-kit";
      asd = ./cl-resilience-kit.asd;
      root = ./.;
      docs.root = ./docs;
      timeoutSeconds = testTimeoutSeconds;
      killAfterSeconds = terminationGraceSeconds;

      lispDependencies = ctx: [
        cl-boundary-kit.packages.${ctx.system}.cl-boundary-kit
        cl-concurrent-kit.packages.${ctx.system}.cl-concurrent-kit
        cl-date-kit.packages.${ctx.system}.cl-date-kit
      ];

      lispCheckDependencies = ctx: [
        cl-weave.packages.${ctx.system}.cl-weave
      ];

      treefmt.evalModule = treefmt-nix.lib.evalModule;

      # Keep the generic report available to CI and to developers. The report
      # uses this system's own test operation and never treats an empty test
      # selection as success because t/runner.lisp rejects one.
      extraOutputs = ctx: {
        packages.coverage = ctx.cl.mkCoverageReport {
          drv = ctx.package;
          timeoutSeconds = coverageTimeoutSeconds;
          killAfterSeconds = terminationGraceSeconds;
        };
        checks.coverage = ctx.cl.mkCoverageReport {
          drv = ctx.package;
          timeoutSeconds = coverageTimeoutSeconds;
          killAfterSeconds = terminationGraceSeconds;
        };
      };
    };
}
