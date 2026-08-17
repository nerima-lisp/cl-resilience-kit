{
  description = "Composable resilience primitives for Common Lisp on top of Nerima Lisp packages";

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

    # Keep the structural editing/lint tool pinned to a release tag so
    # `nix develop` and `nix flake check` do not silently drift on update.
    paredit-cli = {
      url = "github:nerima-lisp/paredit-cli/v1.6.0";
      inputs.nixpkgs.follows = "nixpkgs";
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

    # cl-dataflow-kit's runtime depends on cl-prolog-kit. Both can be consumed as
    # source trees here, which avoids requiring an upstream per-system package
    # output on aarch64-darwin.
    cl-prolog-kit = {
      url = "github:nerima-lisp/cl-prolog-kit/v1.5.0";
      flake = false;
    };

    cl-observability-kit = {
      url = "github:nerima-lisp/cl-observability-kit/v0.1.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.cl-concurrent-kit.follows = "cl-concurrent-kit";
      inputs.cl-date-kit.follows = "cl-date-kit";
      inputs.cl-boundary-kit.follows = "cl-boundary-kit";
      inputs.cl-weave.follows = "cl-weave";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    cl-dataflow-kit = {
      url = "github:nerima-lisp/cl-dataflow-kit/v1.2.0";
      flake = false;
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
      cl-prolog-kit,
      cl-dataflow-kit,
      cl-observability-kit,
      paredit-cli,
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
                                         '("package.lisp")
                                         :test #'string-equal)))
                 (push file discarded-files))))
           table)
          (dolist (file discarded-files)
            (remhash file table)))
      '';
      cl = cl-nix-forge.lib.${nixpkgs.lib.head systems};
      meta = {
        description = "Composable resilience primitives for Common Lisp on top of Nerima Lisp packages";
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

      lispCheckDependencies =
        ctx:
        let
          prolog = ctx.cl.lispDerivation {
            lispSystem = "cl-prolog-kit";
            version = ctx.cl.fromAsdSystem "${cl-prolog-kit}/cl-prolog-kit.asd";
            src = cl-prolog-kit;
          };
          dataflow = ctx.cl.lispDerivation {
            lispSystem = "cl-dataflow-kit";
            version = ctx.cl.fromAsdSystem "${cl-dataflow-kit}/cl-dataflow-kit.asd";
            src = cl-dataflow-kit;
            lispDependencies = [
              prolog
              cl-concurrent-kit.packages.${ctx.system}.cl-concurrent-kit
            ];
          };
        in
        [
          dataflow
          cl-observability-kit.packages.${ctx.system}.cl-observability-kit
          cl-weave.packages.${ctx.system}.cl-weave
        ];

      # SBCL records the absolute source pathname in every FASL. Nix gives
      # each derivation a different temporary build directory. Compile from a
      # writable path private to this build so concurrent derivations cannot
      # delete or chmod each other's source while retaining the dependency
      # registry assembled by cl-nix-forge. The package output still contains
      # source plus FASLs.
      packageArgs = ctx: {
        preBuild = ''
          sourceRoot="$PWD"
          buildRoot="$TMPDIR/cl-resilience-kit-source-$name"
          mkdir -p -m 0700 "$buildRoot"
          find "$buildRoot" -mindepth 1 -depth -delete
          cp -R --no-preserve=mode "$sourceRoot"/. "$buildRoot"/
          chmod -R u+rwX "$buildRoot"
          cd "$buildRoot"

          registryValue="''${CL_SOURCE_REGISTRY:-}"
          registrySuffix="''${registryValue#"$sourceRoot"}"
          if [ "$registrySuffix" = "$registryValue" ]; then
            export CL_SOURCE_REGISTRY="$PWD''${registryValue:+:$registryValue}"
          else
            export CL_SOURCE_REGISTRY="$PWD$registrySuffix"
          fi
        '';
        # sb-cover derives source-page names from absolute pathnames.  The
        # private build root above is intentionally unique, so normalize the
        # generated report only after sb-cover has finished writing it.
        postCheck = ''
                    if [ -d cl-nix-forge-coverage-report ]; then
                      ${
                        nixpkgs.legacyPackages.${ctx.system}.perl
                      }/bin/perl - cl-nix-forge-coverage-report <<'PERL'
          use strict;
          use warnings;

          my ($report_directory) = @ARGV;
          my @pages = glob "$report_directory/*.html";
          die "sb-cover report contains no HTML pages\n" unless @pages;

          my (%renames, %source_roots, %contents);
          for my $page (@pages) {
              open my $input, '<', $page or die "cannot read $page: $!\n";
              local $/;
              my $content = <$input>;
              close $input or die "cannot close $page: $!\n";
              $contents{$page} = $content;

              next unless $content =~ m{Coverage report:\s+(.*/src/)([^ <]+)\s+<br}s;
              my ($source_root, $relative_source) = ($1, $2);
              $source_roots{$source_root} = 1;
              (my $stable_name = "source-$relative_source.html") =~ s{/}{--}g;
              $renames{(split m{/}, $page)[-1]} = $stable_name;
          }

          die "sb-cover report contains no source pages\n" unless %renames;
          die "sb-cover report contains multiple source roots\n" unless keys(%source_roots) == 1;
          my ($source_root) = keys %source_roots;

          for my $page (@pages) {
              my $content = $contents{$page};
              $content =~ s/\Q$source_root\E/src\//g;
              for my $old_name (keys %renames) {
                  $content =~ s/\Q$old_name\E/$renames{$old_name}/g;
              }
              open my $output, '>', $page or die "cannot write $page: $!\n";
              print {$output} $content or die "cannot write $page: $!\n";
              close $output or die "cannot close $page: $!\n";
          }

          for my $old_name (keys %renames) {
              my $old_path = "$report_directory/$old_name";
              my $new_path = "$report_directory/$renames{$old_name}";
              die "stable coverage page already exists: $new_path\n" if -e $new_path;
              rename $old_path, $new_path or die "cannot rename $old_path: $!\n";
          }
          PERL
                    fi
        '';
      };

      treefmt.evalModule = treefmt-nix.lib.evalModule;

      # Match the validator set used by sibling nerima-lisp repositories so
      # structural errors can be reproduced locally from the dev shell.
      devShellPackages = ctx: [
        ctx.pkgs.perl
        paredit-cli.packages.${ctx.system}.default
      ];

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
        checks.paredit-lint = paredit-cli.lib.${ctx.system}.mkLintCheck {
          inherit (ctx) src;
          name = "cl-resilience-kit-paredit-lint";
        };
      };
    };
}
