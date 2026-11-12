// Pulsar — minimalist self-hosted monitor and remote kill-switch for CLI agents.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ylnyorulmaz/pulsar/internal/runner"
	"github.com/ylnyorulmaz/pulsar/internal/server"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		os.Exit(runServer(os.Args[2:]))
	case "run":
		os.Exit(runRunner(os.Args[2:]))
	case "version", "-v", "--version":
		fmt.Println("pulsar", version)
		os.Exit(0)
	case "help", "-h", "--help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Pulsar %s — self-hosted CLI agent monitor & kill switch

Usage:
  pulsar server --secret <token> [--addr :8741]
  pulsar run --token <token> [--name <label>] [--admin-url <url>] [--hub-url <url>] -- <command> [args...]

Environment:
  PULSAR_HUB_URL   Default hub URL for pulsar run (fallback: http://127.0.0.1:8741)

Examples:
  pulsar server --secret "$(openssl rand -hex 16)"
  pulsar run --token "$SECRET" --name hermes -- claude
  pulsar run --token "$SECRET" --admin-url http://127.0.0.1:3000 -- npm run build
`, version)
}

func runServer(args []string) int {
	cfg := server.Config{}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--secret":
			i++
			if i >= len(args) {
				log.Fatal("--secret requires a value")
			}
			cfg.Secret = args[i]
		case "--addr":
			i++
			if i >= len(args) {
				log.Fatal("--addr requires a value")
			}
			cfg.Addr = args[i]
		default:
			log.Fatalf("unknown flag %q", args[i])
		}
	}
	if err := server.Run(cfg); err != nil {
		log.Fatal(err)
	}
	return 0
}

func runRunner(args []string) int {
	cfg := runner.Config{}
	var cmdArgs []string
	seenSep := false

	for i := 0; i < len(args); i++ {
		if !seenSep && args[i] == "--" {
			seenSep = true
			cmdArgs = args[i+1:]
			break
		}
		if !seenSep {
			switch args[i] {
			case "--token":
				i++
				cfg.Token = args[i]
			case "--name":
				i++
				cfg.Name = args[i]
			case "--admin-url":
				i++
				cfg.AdminURL = args[i]
			case "--hub-url":
				i++
				cfg.HubURL = args[i]
			default:
				if strings.HasPrefix(args[i], "-") {
					log.Fatalf("unknown flag %q", args[i])
				}
				cmdArgs = args[i:]
				seenSep = true
				break
			}
		}
	}

	if len(cmdArgs) == 0 {
		log.Fatal("missing command after --")
	}
	cfg.Command = cmdArgs

	if err := runner.Run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
	return 0
}
