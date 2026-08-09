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
      coverageEntryPointText = ''
        (load "run-tests.lisp")
        (let* ((root (uiop:ensure-directory-pathname (uiop:getcwd)))
               (source-root (merge-pathnames "src/" root))
               (table (sb-cover::code-coverage-hashtable))
               (discarded-files nil))
          (maphash
           (lambda (file coverage)
             (declare (ignore coverage))
             (let ((source (pathname file)))
               (unless (and (uiop:subpathp source source-root)
                            (not (member (file-namestring source)
                                         '("conditions.lisp" "package.lisp")
                                         :test #'string-equal)))
                 (push file discarded-files))))
           table)
          (dolist (file discarded-files)
            (remhash file table)))
      '';
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

      # SBCL records the absolute source pathname in every FASL. Nix gives
      # each derivation a different temporary build directory, so compiling
      # from the default working tree makes an otherwise identical package
      # differ byte-for-byte between builds. Compile from a stable, sandboxed
      # path while retaining the dependency registry assembled by
      # cl-nix-forge. The package output still contains source plus FASLs.
      packageArgs = _: {
        preBuild = ''
          sourceRoot="$PWD"
          stableRoot=/tmp/cl-resilience-kit-source-v2
          mkdir -p -m 0777 "$stableRoot"
          find "$stableRoot" -mindepth 1 -depth -delete
          cp -R "$sourceRoot"/. "$stableRoot"/
          find "$stableRoot" -mindepth 1 -type d -exec chmod a+rwx {} +
          find "$stableRoot" -mindepth 1 -type f -exec chmod a+rw {} +
          cd "$stableRoot"

          registryValue="''${CL_SOURCE_REGISTRY:-}"
          registrySuffix="''${registryValue#"$sourceRoot"}"
          if [ "$registrySuffix" = "$registryValue" ]; then
            export CL_SOURCE_REGISTRY="$PWD''${registryValue:+:$registryValue}"
          else
            export CL_SOURCE_REGISTRY="$PWD$registrySuffix"
          fi
        '';
      };

      treefmt.evalModule = treefmt-nix.lib.evalModule;

      # Keep the generic report available to CI and to developers. The report
      # uses this system's own test operation and never treats an empty test
      # selection as success because t/runner.lisp rejects one.
      extraOutputs = ctx: {
        packages.coverage = ctx.cl.mkCoverageReport {
          drv = ctx.package;
          entryPointText = coverageEntryPointText;
          timeoutSeconds = coverageTimeoutSeconds;
          killAfterSeconds = terminationGraceSeconds;
        };
        checks.coverage = ctx.cl.mkCoverageReport {
          drv = ctx.package;
          entryPointText = coverageEntryPointText;
          timeoutSeconds = coverageTimeoutSeconds;
          killAfterSeconds = terminationGraceSeconds;
        };
      };
    };
}
