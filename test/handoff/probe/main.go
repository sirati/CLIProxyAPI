// Command probe supports the NixOS VM test of seamless upgrades.
//
//	probe upstreams -claude ADDR -codex ADDR -ca FILE
//	    serves the fake Claude (HTTP) and Codex (TLS websocket) upstreams and
//	    writes the CA the proxy must trust; GET /_probe/stats on the Claude
//	    address reports what the Codex upstream saw.
//	probe client -proxy URL -key KEY -report FILE
//	    runs back-to-back Claude streams, a keep-alive request loop and a
//	    Codex session until SIGTERM, then writes a JSON report.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/test/handoff/fakeupstream"
	"github.com/router-for-me/CLIProxyAPI/v8/test/handoff/workload"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: probe upstreams|client [flags]")
	}
	switch os.Args[1] {
	case "upstreams":
		runUpstreams(os.Args[2:])
	case "client":
		runClient(os.Args[2:])
	default:
		log.Fatalf("unknown mode %q", os.Args[1])
	}
}

func runUpstreams(args []string) {
	flags := flag.NewFlagSet("upstreams", flag.ExitOnError)
	claudeAddr := flags.String("claude", "127.0.0.1:18001", "Claude upstream address")
	codexAddr := flags.String("codex", "127.0.0.1:18002", "Codex upstream address (TLS)")
	caPath := flags.String("ca", "/tmp/probe-ca.pem", "where to write the CA certificate")
	_ = flags.Parse(args)

	cert, certPEM, errCert := selfSigned()
	if errCert != nil {
		log.Fatal(errCert)
	}
	if errWrite := os.WriteFile(*caPath, certPEM, 0o644); errWrite != nil {
		log.Fatal(errWrite)
	}
	claude, codex := &fakeupstream.Claude{}, &fakeupstream.Codex{}
	mux := http.NewServeMux()
	mux.Handle("/", claude)
	mux.HandleFunc("/_probe/stats", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"codex_connections": codex.Connections(),
			"codex_errors":      codex.ProtocolErrors(),
			"claude_requests":   claude.Requests(),
		})
	})
	go func() { log.Fatal(http.ListenAndServe(*claudeAddr, mux)) }()
	listener, errListen := tls.Listen("tcp", *codexAddr, &tls.Config{Certificates: []tls.Certificate{cert}})
	if errListen != nil {
		log.Fatal(errListen)
	}
	log.Printf("upstreams ready: claude=%s codex=%s", *claudeAddr, *codexAddr)
	log.Fatal(http.Serve(listener, codex))
}

func selfSigned() (tls.Certificate, []byte, error) {
	key, errKey := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if errKey != nil {
		return tls.Certificate{}, nil, errKey
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "probe"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, errCreate := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if errCreate != nil {
		return tls.Certificate{}, nil, errCreate
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, _ := x509.MarshalECPrivateKey(key)
	cert, errPair := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return cert, certPEM, errPair
}

// report is what the VM test checks: every list must be empty, FreshConns 0
// and CodexConns a single connection.
type report struct {
	Streams        int      `json:"streams"`
	StreamErrors   []string `json:"stream_errors"`
	Requests       int      `json:"requests"`
	RequestErrors  []string `json:"request_errors"`
	FreshConns     int      `json:"fresh_connections"`
	Turns          int      `json:"turns"`
	TurnErrors     []string `json:"turn_errors"`
	CodexConns     []string `json:"codex_conns"`
	mu             sync.Mutex
	codexConnsSeen map[string]bool
}

func (r *report) fail(list *[]string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(*list) < 20 {
		*list = append(*list, fmt.Sprintf("%s %v", time.Now().Format("15:04:05.000"), err))
	}
}

func runClient(args []string) {
	flags := flag.NewFlagSet("client", flag.ExitOnError)
	proxy := flags.String("proxy", "http://127.0.0.1:8317", "proxy base URL")
	key := flags.String("key", "client-key", "client API key")
	reportPath := flags.String("report", "/tmp/probe-report.json", "where to write the report")
	_ = flags.Parse(args)

	stop := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-signals; close(stop) }()

	r := &report{codexConnsSeen: map[string]bool{}}
	messages := *proxy + "/v1/messages"
	var wg sync.WaitGroup
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// Back-to-back 3 s streams: one is always running when the switch comes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{}
		for !stopped() {
			body, _, errStream := workload.ClaudeStream(client, messages, *key, "stream:30")
			if errStream == nil && !strings.Contains(body, "[29|") {
				errStream = fmt.Errorf("stream incomplete: %d bytes", len(body))
			}
			r.mu.Lock()
			r.Streams++
			r.mu.Unlock()
			if errStream != nil {
				r.fail(&r.StreamErrors, errStream)
			}
		}
	}()

	// One keep-alive connection that must survive every switch.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1}}
		first := true
		for !stopped() {
			_, reused, errReq := workload.ClaudeStream(client, messages, *key, "ping")
			r.mu.Lock()
			r.Requests++
			if !first && !reused && errReq == nil {
				r.FreshConns++
			}
			r.mu.Unlock()
			first = false
			if errReq != nil {
				r.fail(&r.RequestErrors, errReq)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// A Codex session continuing one conversation on one upstream connection.
	wg.Add(1)
	go func() {
		defer wg.Done()
		session, errDial := workload.DialCodex(strings.Replace(*proxy, "http", "ws", 1)+"/v1/responses", *key)
		if errDial != nil {
			r.fail(&r.TurnErrors, errDial)
			return
		}
		defer func() { _ = session.Close() }()
		for n := 1; !stopped(); n++ {
			text, errTurn := session.Turn(fmt.Sprintf("turn %d", n))
			r.mu.Lock()
			r.Turns++
			r.mu.Unlock()
			if errTurn != nil {
				r.fail(&r.TurnErrors, errTurn)
				return
			}
			conn, _, _ := strings.Cut(text, " ")
			if want := fmt.Sprintf("turn=%d", n); !strings.HasSuffix(text, want) {
				r.fail(&r.TurnErrors, fmt.Errorf("got %q, want %s", text, want))
			}
			r.mu.Lock()
			if !r.codexConnsSeen[conn] {
				r.codexConnsSeen[conn] = true
				r.CodexConns = append(r.CodexConns, conn)
			}
			r.mu.Unlock()
			time.Sleep(300 * time.Millisecond)
		}
	}()

	wg.Wait()
	r.mu.Lock()
	data, _ := json.MarshalIndent(r, "", "  ")
	r.mu.Unlock()
	if errWrite := os.WriteFile(*reportPath+".tmp", data, 0o644); errWrite != nil {
		log.Fatal(errWrite)
	}
	if errRename := os.Rename(*reportPath+".tmp", *reportPath); errRename != nil {
		log.Fatal(errRename)
	}
}
