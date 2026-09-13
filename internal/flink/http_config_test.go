package flink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfiguredClientsSendAuthentication(t *testing.T) {
	t.Run("bearer token to Flink", func(t *testing.T) {
		client, err := NewClientWithConfig("http://flink.test", HTTPClientConfig{BearerToken: "cluster-token"})
		if err != nil {
			t.Fatal(err)
		}
		transport := client.http.Transport.(authenticatedTransport)
		transport.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if got := request.Header.Get("Authorization"); got != "Bearer cluster-token" {
				t.Errorf("Authorization = %q", got)
			}
			return testHTTPResponse(request, http.StatusOK, `{"taskmanagers":1,"slots-total":4,"slots-available":2}`), nil
		})
		client.http.Transport = transport
		if _, err := client.ClusterOverview(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("basic auth to SQL Gateway", func(t *testing.T) {
		client, err := NewSQLGatewayClientWithConfig("http://gateway.test", HTTPClientConfig{
			BasicUsername: "operator",
			BasicPassword: "secret",
		})
		if err != nil {
			t.Fatal(err)
		}
		transport := client.http.Transport.(authenticatedTransport)
		transport.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			username, password, ok := request.BasicAuth()
			if !ok || username != "operator" || password != "secret" {
				t.Errorf("basic auth = (%q, %q, %t)", username, password, ok)
			}
			return testHTTPResponse(request, http.StatusOK, `{"productName":"Apache Flink","version":"2.3.0"}`), nil
		})
		client.http.Transport = transport
		if _, err := client.Info(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("credentials do not follow cross-origin redirects", func(t *testing.T) {
		client, err := NewClientWithConfig("https://flink.test", HTTPClientConfig{BearerToken: "cluster-token"})
		if err != nil {
			t.Fatal(err)
		}
		requests := 0
		transport := client.http.Transport.(authenticatedTransport)
		transport.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			if request.URL.Host != "flink.test" {
				t.Fatalf("credentials were sent to redirected host %q", request.URL.Host)
			}
			response := testHTTPResponse(request, http.StatusFound, "")
			response.Header.Set("Location", "http://attacker.test/capture")
			return response, nil
		})
		client.http.Transport = transport
		response, err := client.http.Get("https://flink.test/overview")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		if requests != 1 || response.StatusCode != http.StatusFound {
			t.Fatalf("redirect requests = %d status = %d", requests, response.StatusCode)
		}
	})
}

func TestHTTPClientConfigurationValidationAndStructuredErrors(t *testing.T) {
	invalid := []HTTPClientConfig{
		{BasicUsername: "operator", BearerToken: "token"},
		{BasicPassword: "secret"},
		{ClientCertificateFile: "client.pem"},
		{ClientKeyFile: "client-key.pem"},
	}
	for _, config := range invalid {
		if _, err := NewClientWithConfig("https://flink.test", config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
	if _, err := NewClient("ftp://flink.test"); err == nil {
		t.Fatal("non-HTTP endpoint scheme was accepted")
	}

	client := testClient(t, func(*http.Request) (string, int) {
		return "token expired", http.StatusUnauthorized
	})
	_, err := client.ClusterOverview(context.Background())
	var requestError *RequestError
	if !errors.As(err, &requestError) || requestError.StatusCode != http.StatusUnauthorized || requestError.Body != "token expired" {
		t.Fatalf("request error = %#v, %v", requestError, err)
	}
}

func TestEndpointValidationDoesNotExposeCredentials(t *testing.T) {
	constructors := []struct {
		name string
		new  func(string) (string, error)
	}{
		{name: "Flink", new: func(endpoint string) (string, error) {
			client, err := NewClient(endpoint)
			if err != nil {
				return "", err
			}
			return client.Endpoint(), nil
		}},
		{name: "SQL Gateway", new: func(endpoint string) (string, error) {
			client, err := NewSQLGatewayClient(endpoint)
			if err != nil {
				return "", err
			}
			return client.Endpoint(), nil
		}},
	}
	for _, constructor := range constructors {
		t.Run(constructor.name, func(t *testing.T) {
			for _, endpoint := range []string{
				"", "http://:8081", "ftp://flink.test",
				"http://operator:secret-password@flink.test",
				"http://operator:secret-password@%bad",
				"http://flink.test?token=secret-password", "http://flink.test?",
				"http://flink.test#secret-password", "http://flink.test#",
			} {
				_, err := constructor.new(endpoint)
				if err == nil {
					t.Errorf("accepted invalid endpoint %q", endpoint)
				} else if strings.Contains(err.Error(), "secret-password") {
					t.Errorf("endpoint error exposed credentials: %v", err)
				}
			}
			got, err := constructor.new(" flink.test:8081/proxy/ ")
			if err != nil || got != "http://flink.test:8081/proxy" {
				t.Fatalf("normalized endpoint = %q, %v", got, err)
			}
		})
	}
}

func TestHTTPClientStopsSameOriginRedirectLoops(t *testing.T) {
	client, err := newHTTPClient(time.Second, HTTPClientConfig{BearerToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	transport := client.Transport.(authenticatedTransport)
	transport.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if requests > 10 {
			return nil, errors.New("redirect limit was not enforced")
		}
		response := testHTTPResponse(request, http.StatusFound, "")
		response.Header.Set("Location", "/loop")
		return response, nil
	})
	client.Transport = transport
	_, err = client.Get("https://flink.test/loop")
	if err == nil || !strings.Contains(err.Error(), "stopped after 10 redirects") || requests != 10 {
		t.Fatalf("redirect loop made %d requests, error = %v", requests, err)
	}
}

func TestResponseErrorPreservesPartialBodyAndReadFailure(t *testing.T) {
	readFailure := errors.New("socket closed while reading")
	response := &http.Response{
		StatusCode: http.StatusBadGateway,
		Status:     "502 Bad Gateway",
		Body:       &failingReadCloser{err: readFailure},
	}

	requestError := newResponseError(response, "Flink", http.MethodGet, "/overview", 1024)
	if requestError.Body != "partial response" || !errors.Is(requestError, readFailure) {
		t.Fatalf("response error = %#v", requestError)
	}
	message := requestError.Error()
	for _, want := range []string{"502 Bad Gateway", "partial response", "read error response", "socket closed while reading"} {
		if !strings.Contains(message, want) {
			t.Fatalf("error %q does not contain %q", message, want)
		}
	}
}

type failingReadCloser struct {
	err  error
	sent bool
}

func (reader *failingReadCloser) Read(buffer []byte) (int, error) {
	if reader.sent {
		return 0, reader.err
	}
	reader.sent = true
	return copy(buffer, "partial response"), reader.err
}

func (*failingReadCloser) Close() error { return nil }

func testHTTPResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

func testCertificateAuthority(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "flink-tui test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
}

func issueTestCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, template x509.Certificate) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, &template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writeTestPEM(t *testing.T, directory, name string, contents []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
