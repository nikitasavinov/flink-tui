package flink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPSConnectionsIntegration(t *testing.T) {
	pki := newConnectionTestPKI(t)
	foreign := newConnectionTestPKI(t)
	trusted := HTTPClientConfig{CACertificateFile: pki.client.CACertificateFile}
	basic := trusted
	basic.BasicUsername, basic.BasicPassword = "operator", "test-password"
	bearer := trusted
	bearer.BearerToken = "test-token"
	wrongPassword := basic
	wrongPassword.BasicPassword = "wrong-password"
	wrongToken := bearer
	wrongToken.BearerToken = "wrong-token"
	foreignClient := foreign.client
	foreignClient.CACertificateFile = trusted.CACertificateFile
	basicHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte("operator:test-password"))

	for _, test := range []struct {
		name        string
		config      HTTPClientConfig
		certificate *tls.Certificate
		auth        string
		mutualTLS   bool
		maxVersion  uint16
		wantStatus  int
		wantTLSFail bool
	}{
		{name: "private CA", config: trusted},
		{name: "TLS 1.2", config: trusted, maxVersion: tls.VersionTLS12},
		{name: "untrusted server", wantTLSFail: true},
		{name: "wrong CA", config: HTTPClientConfig{CACertificateFile: foreign.client.CACertificateFile}, wantTLSFail: true},
		{name: "wrong hostname", config: trusted, certificate: &pki.wrongHost, wantTLSFail: true},
		{name: "expired server", config: trusted, certificate: &pki.expiredServer, wantTLSFail: true},
		{name: "skip verification", config: HTTPClientConfig{InsecureSkipVerify: true}},
		{name: "basic authentication", config: basic, auth: basicHeader},
		{name: "wrong password", config: wrongPassword, auth: basicHeader, wantStatus: http.StatusUnauthorized},
		{name: "missing basic credentials", config: trusted, auth: basicHeader, wantStatus: http.StatusUnauthorized},
		{name: "bearer authentication", config: bearer, auth: "Bearer test-token"},
		{name: "wrong token", config: wrongToken, auth: "Bearer test-token", wantStatus: http.StatusUnauthorized},
		{name: "missing bearer token", config: trusted, auth: "Bearer test-token", wantStatus: http.StatusUnauthorized},
		{name: "mutual TLS", config: pki.client, mutualTLS: true},
		{name: "missing client certificate", config: trusted, mutualTLS: true, wantTLSFail: true},
		{name: "untrusted client certificate", config: foreignClient, mutualTLS: true, wantTLSFail: true},
		{name: "expired client certificate", config: pki.expiredClient, mutualTLS: true, wantTLSFail: true},
		{name: "skip verification still requires client certificate", config: HTTPClientConfig{InsecureSkipVerify: true}, mutualTLS: true, wantTLSFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, service := range []string{"Flink", "SQL Gateway"} {
				t.Run(service, func(t *testing.T) {
					serverTLS := pki.serverConfig(test.mutualTLS)
					serverTLS.MaxVersion = test.maxVersion
					if test.certificate != nil {
						serverTLS.Certificates = []tls.Certificate{*test.certificate}
					}
					var requests atomic.Int32
					server := startConnectionTestServer(t, serverTLS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests.Add(1)
						if r.TLS == nil || !r.TLS.HandshakeComplete || r.TLS.Version < tls.VersionTLS12 {
							t.Error("request did not complete a TLS 1.2+ handshake")
						}
						if test.maxVersion != 0 && r.TLS.Version != test.maxVersion {
							t.Errorf("TLS version = %x, want %x", r.TLS.Version, test.maxVersion)
						}
						if test.mutualTLS && (len(r.TLS.VerifiedChains) == 0 ||
							r.TLS.VerifiedChains[0][0].Subject.CommonName != "flink-tui test client") {
							t.Error("server did not authenticate the client certificate")
						}
						if test.auth != "" && r.Header.Get("Authorization") != test.auth {
							http.Error(w, "authentication required", http.StatusUnauthorized)
							return
						}
						path := "/cluster/overview"
						if service == "SQL Gateway" {
							path = "/cluster/v1/info"
						}
						if r.URL.Path != path {
							t.Errorf("request path = %q, want %q", r.URL.Path, path)
							http.NotFound(w, r)
							return
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, "{\"flink-version\":\"secure-fixture\",\"version\":\"secure-fixture\"}")
					}))
					version, err := connectionTestVersion(t, service, server.URL+"/cluster", test.config)
					switch {
					case test.wantTLSFail:
						var requestError *RequestError
						if !errors.As(err, &requestError) || requestError.Err == nil ||
							!strings.Contains(err.Error(), "tls:") || requests.Load() != 0 {
							t.Fatalf("expected TLS rejection before HTTP, requests=%d error=%v", requests.Load(), err)
						}
					case test.wantStatus != 0:
						var requestError *RequestError
						if !errors.As(err, &requestError) || requestError.StatusCode != test.wantStatus || requests.Load() != 1 {
							t.Fatalf("expected HTTP %d, requests=%d error=%v", test.wantStatus, requests.Load(), err)
						}
					default:
						if err != nil || version != "secure-fixture" || requests.Load() != 1 {
							t.Fatalf("version=%q requests=%d error=%v", version, requests.Load(), err)
						}
					}
				})
			}
		})
	}
}

func TestE2ESecureConnections(t *testing.T) {
	restEndpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	sqlEndpoint := os.Getenv("FLINK_TUI_E2E_SQL_ENDPOINT")
	if restEndpoint == "" || sqlEndpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT and FLINK_TUI_E2E_SQL_ENDPOINT or run make test-e2e")
	}
	restPKI, sqlPKI := newConnectionTestPKI(t), newConnectionTestPKI(t)
	for _, test := range []struct {
		name      string
		mutualTLS bool
		insecure  bool
	}{
		{name: "private CA"},
		{name: "mutual TLS", mutualTLS: true},
		{name: "skip verification", insecure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, restAuth := range []string{"basic", "bearer"} {
				t.Run("REST "+restAuth, func(t *testing.T) {
					restConfig, sqlConfig := restPKI.client, sqlPKI.client
					restConfig.InsecureSkipVerify, sqlConfig.InsecureSkipVerify = test.insecure, test.insecure
					if test.insecure {
						restConfig.CACertificateFile, sqlConfig.CACertificateFile = "", ""
					}
					if !test.mutualTLS {
						restConfig.ClientCertificateFile, restConfig.ClientKeyFile = "", ""
						sqlConfig.ClientCertificateFile, sqlConfig.ClientKeyFile = "", ""
					}
					restHeader, sqlHeader := "Bearer rest-token", "Bearer sql-token"
					if restAuth == "basic" {
						restConfig.BasicUsername, restConfig.BasicPassword = "rest-user", "rest-password"
						restHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte("rest-user:rest-password"))
						sqlConfig.BearerToken = "sql-token"
					} else {
						restConfig.BearerToken = "rest-token"
						sqlConfig.BasicUsername, sqlConfig.BasicPassword = "sql-user", "sql-password"
						sqlHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte("sql-user:sql-password"))
					}
					restProxy := connectionTestProxy(t, restPKI.serverConfig(test.mutualTLS), restEndpoint, "/flink", restHeader)
					sqlProxy := connectionTestProxy(t, sqlPKI.serverConfig(test.mutualTLS), sqlEndpoint, "/sql", sqlHeader)
					restURL, sqlURL := restProxy.URL+"/flink", sqlProxy.URL+"/sql"

					for _, service := range []struct {
						name, endpoint string
						config         HTTPClientConfig
					}{
						{name: "Flink", endpoint: restURL, config: restConfig},
						{name: "SQL Gateway", endpoint: sqlURL, config: sqlConfig},
					} {
						badConfig := service.config
						badConfig.BasicUsername, badConfig.BasicPassword, badConfig.BearerToken = "", "", "wrong-token"
						_, err := connectionTestVersion(t, service.name, service.endpoint, badConfig)
						var requestError *RequestError
						if !errors.As(err, &requestError) || requestError.StatusCode != http.StatusUnauthorized {
							t.Fatalf("%s proxy did not reject wrong credentials: %v", service.name, err)
						}
					}

					ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
					defer cancel()
					var restVersion, sqlVersion string
					waitForLiveAPI(t, ctx, "authenticated Flink and SQL Gateway", func() (bool, error) {
						var err error
						restVersion, err = connectionTestVersion(t, "Flink", restURL, restConfig)
						if err != nil {
							return false, err
						}
						sqlVersion, err = connectionTestVersion(t, "SQL Gateway", sqlURL, sqlConfig)
						return restVersion != "" && sqlVersion != "", err
					})
					if restVersion != sqlVersion {
						t.Fatalf("REST version %q differs from SQL Gateway %q", restVersion, sqlVersion)
					}
					if expected := os.Getenv("FLINK_TUI_EXPECT_VERSION"); expected != "" && restVersion != expected {
						t.Fatalf("connected to Flink %s, expected %s", restVersion, expected)
					}
					t.Logf("authenticated HTTPS requests reached Flink %s", restVersion)
					checkConnectionTestSQL(t, sqlURL, sqlConfig)
				})
			}
		})
	}
}

func connectionTestVersion(t *testing.T, service, endpoint string, config HTTPClientConfig) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if service == "Flink" {
		client, err := NewClientWithConfig(endpoint, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.http.CloseIdleConnections()
		overview, err := client.ClusterOverview(ctx)
		return overview.FlinkVersion, err
	}
	client, err := NewSQLGatewayClientWithConfig(endpoint, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.http.CloseIdleConnections()
	info, err := client.Info(ctx)
	return info.Version, err
}

func checkConnectionTestSQL(t *testing.T, endpoint string, config HTTPClientConfig) {
	t.Helper()
	client, err := NewSQLGatewayClientWithConfig(endpoint, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	session, err := client.OpenSession(ctx, "flink-tui-secure-connection-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	operation := ""
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if operation != "" {
			if err := client.CloseOperation(cleanupContext, session, operation); err != nil {
				t.Errorf("close SQL operation: %v", err)
			}
		}
		if err := client.CloseSession(cleanupContext, session); err != nil {
			t.Errorf("close SQL session: %v", err)
		}
		client.http.CloseIdleConnections()
	})
	operation, err = client.Execute(ctx, session, "SELECT 1 AS answer")
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	nextURI := ""
	waitForLiveAPI(t, ctx, "SQL results through authenticated TLS proxy", func() (bool, error) {
		result, err := client.FetchResults(ctx, session, operation, nextURI)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range result.Rows {
			rows = append(rows, row.Fields...)
		}
		nextURI = result.NextURI
		if result.ResultType == "EOS" {
			return true, nil
		}
		if nextURI == "" {
			t.Fatalf("SQL result %q has no continuation URI", result.ResultType)
		}
		return false, nil
	})
	if !slices.Equal(rows, []string{"1"}) {
		t.Fatalf("SQL result = %v, want [1]", rows)
	}
}

func connectionTestProxy(t *testing.T, config *tls.Config, endpoint, prefix, authorization string) *httptest.Server {
	t.Helper()
	target, err := url.Parse(endpoint)
	if err != nil || target.Host == "" {
		t.Fatalf("invalid upstream endpoint %q: %v", endpoint, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target) // #nosec G704 -- Test-only upstreams are the explicitly configured Flink E2E endpoints.
	mux := http.NewServeMux()
	mux.Handle(prefix+"/", http.StripPrefix(prefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != authorization {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		r.Header.Del("Authorization")
		proxy.ServeHTTP(w, r)
	})))
	return startConnectionTestServer(t, config, mux)
}

func startConnectionTestServer(t *testing.T, config *tls.Config, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = config
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

type connectionTestPKI struct {
	server        tls.Certificate
	wrongHost     tls.Certificate
	expiredServer tls.Certificate
	roots         *x509.CertPool
	client        HTTPClientConfig
	expiredClient HTTPClientConfig
}

func newConnectionTestPKI(t *testing.T) connectionTestPKI {
	t.Helper()
	ca, key, caPEM := testCertificateAuthority(t)
	directory := t.TempDir()
	caPath := writeTestPEM(t, directory, "ca.pem", caPEM)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	var serial int64 = 1
	certificate := func(name string, template x509.Certificate) (tls.Certificate, HTTPClientConfig) {
		serial++
		template.SerialNumber = big.NewInt(serial)
		certPEM, keyPEM := issueTestCertificate(t, ca, key, template)
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return pair, HTTPClientConfig{
			CACertificateFile:     caPath,
			ClientCertificateFile: writeTestPEM(t, directory, name+".pem", certPEM),
			ClientKeyFile:         writeTestPEM(t, directory, name+"-key.pem", keyPEM),
		}
	}
	template := x509.Certificate{
		Subject:     pkix.Name{CommonName: "flink-tui test server"},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	pki := connectionTestPKI{roots: roots}
	pki.server, _ = certificate("server", template)
	template.IPAddresses = []net.IP{net.IPv4(192, 0, 2, 1)}
	pki.wrongHost, _ = certificate("wrong-host", template)
	template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	template.NotBefore, template.NotAfter = time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
	pki.expiredServer, _ = certificate("expired-server", template)
	template.Subject.CommonName = "flink-tui test client"
	template.IPAddresses = nil
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	template.NotBefore, template.NotAfter = time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	_, pki.client = certificate("client", template)
	template.NotBefore, template.NotAfter = time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
	_, pki.expiredClient = certificate("expired-client", template)
	return pki
}

func (pki connectionTestPKI) serverConfig(mutualTLS bool) *tls.Config {
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{pki.server},
	}
	if mutualTLS {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = pki.roots
	}
	return config
}
