// Command reckon is a single static binary exposing the reckon-go API as
// JSON-emitting subcommands. See plans/DESIGN_RECKON_CLI.md for the contract.
//
// Layout: main() is a thin wrapper over run(), which is fully testable —
// args in, JSON out, exit code returned. No third-party CLI deps (stdlib
// flag only) so nothing leaks into reckon-go's module graph (DESIGN §7-§8).
package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
	"codeberg.org/reckon-db-org/reckon-go/cmd/reckon/encode"
	"google.golang.org/grpc"
)

// version is stamped at build time via -ldflags "-X main.version=<tag>"
// (see scripts/build-release.sh). "dev" for plain `go build`/`go install`.
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// opts holds parsed global flags shared by every command.
type opts struct {
	endpoint    string
	store       string
	timeout     time.Duration
	bytes       encode.Bytes
	pretty      bool
	showVersion bool
	plaintext   bool
	caFile      string
	serverName  string
}

// handler is one command leaf: it owns its command-flags (parsed from args),
// reads payload input from in (stdin), and writes its result to out. A
// non-nil error must be a *cliError.
type handler func(ctx context.Context, o *opts, args []string, in io.Reader, out io.Writer) error

func run(ctx context.Context, argv []string, in io.Reader, stdout, stderr io.Writer) int {
	o, rest, err := parseGlobal(argv)
	if err != nil {
		return emitErr(stderr, err)
	}
	if o.showVersion {
		_ = writeJSON(stdout, map[string]any{"client": version, "api_compat": "reckon.gateway.v1"}, o.pretty)
		return 0
	}
	if len(rest) < 2 {
		return emitErr(stderr, usageErr("expected: reckon [flags] <group> <command> [args]"))
	}
	key := rest[0] + " " + rest[1]
	h, ok := routes[key]
	if !ok {
		return emitErr(stderr, usageErr("unknown command %q", key))
	}
	if err := h(ctx, o, rest[2:], in, stdout); err != nil {
		return emitErr(stderr, err)
	}
	return 0
}

func parseGlobal(argv []string) (*opts, []string, error) {
	fs := flag.NewFlagSet("reckon", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	def := func(env, fallback string) string {
		if v := os.Getenv(env); v != "" {
			return v
		}
		return fallback
	}

	var epLong, epShort string
	fs.StringVar(&epLong, "endpoint", def("RECKON_ENDPOINT", "localhost:50051"), "gateway host:port")
	fs.StringVar(&epShort, "e", "", "gateway host:port (alias)")
	var stLong, stShort string
	fs.StringVar(&stLong, "store", def("RECKON_STORE", ""), "store id")
	fs.StringVar(&stShort, "s", "", "store id (alias)")
	timeout := fs.Duration("timeout", parseDur(def("RECKON_TIMEOUT", "5s")), "per-RPC deadline")
	bytesFlag := fs.String("bytes", def("RECKON_BYTES", "auto"), "byte field rendering: auto|base64")
	pretty := fs.Bool("pretty", false, "pretty-print unary JSON")
	verLong := fs.Bool("version", false, "print client version and exit")
	verShort := fs.Bool("V", false, "print client version and exit (alias)")
	plaintext := fs.Bool("plaintext", os.Getenv("RECKON_PLAINTEXT") == "1",
		"dial without TLS (unauthenticated, tamperable; lab/loopback only)")
	caFile := fs.String("ca", def("RECKON_CA", ""),
		"PEM bundle to verify the gateway against (instead of system roots)")
	serverName := fs.String("server-name", def("RECKON_SERVER_NAME", ""),
		"override the TLS verification/SNI hostname")

	if err := fs.Parse(argv); err != nil {
		return nil, nil, usageErr("flag error: %v", err)
	}

	bm, ok := encode.ParseBytes(*bytesFlag)
	if !ok {
		return nil, nil, usageErr("invalid --bytes %q (want auto|base64)", *bytesFlag)
	}
	o := &opts{
		endpoint:    pick(epShort, epLong),
		store:       pick(stShort, stLong),
		timeout:     *timeout,
		bytes:       bm,
		pretty:      *pretty,
		showVersion: *verLong || *verShort,
		plaintext:   *plaintext,
		caFile:      *caFile,
		serverName:  *serverName,
	}
	if o.plaintext && (o.caFile != "" || o.serverName != "") {
		return nil, nil, usageErr("--plaintext conflicts with --ca/--server-name")
	}
	return o, fs.Args(), nil
}

// flagsThenArgs parses fs against args allowing flags and positionals to be
// interspersed. Stdlib flag stops at the first non-flag token, so we loop:
// consume leading flags, take one positional, repeat. Handles both
// `read <stream> --count 1` and `read --count 1 <stream>`.
func flagsThenArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

func pick(short, long string) string {
	if short != "" {
		return short
	}
	return long
}

func parseDur(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

func (o *opts) requireStore() error {
	if o.store == "" {
		return usageErr("this command requires --store (or RECKON_STORE)")
	}
	return nil
}

// dial opens a client for the configured endpoint. Default transport is
// TLS against the system roots; --plaintext is the explicit opt-out and
// --ca/--server-name cover self-signed / private-CA gateways.
func (o *opts) dial(ctx context.Context) (*reckon.Client, error) {
	dialOpts, err := o.transport()
	if err != nil {
		return nil, &cliError{"usage", err.Error(), -1, 2}
	}
	c, err := reckon.Connect(ctx, o.endpoint, dialOpts...)
	if err != nil {
		return nil, &cliError{"unreachable", err.Error(), -1, 6}
	}
	return c, nil
}

func (o *opts) transport() ([]grpc.DialOption, error) {
	switch {
	case o.plaintext:
		return []grpc.DialOption{reckon.Insecure()}, nil
	case o.caFile != "":
		opt, err := reckon.TLSWithCA(o.caFile, o.serverName)
		if err != nil {
			return nil, err
		}
		return []grpc.DialOption{opt}, nil
	case o.serverName != "":
		return []grpc.DialOption{reckon.TLSWithServerName(o.serverName)}, nil
	default:
		return nil, nil // Connect's default: TLS, system roots
	}
}

// rpcCtx derives a per-RPC deadline from the signal-aware parent.
func (o *opts) rpcCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, o.timeout)
}
