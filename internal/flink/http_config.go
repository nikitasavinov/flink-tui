package flink

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// HTTPClientConfig configures authentication and private-PKI support for a
// Flink REST endpoint. Basic authentication and bearer authentication are
// mutually exclusive. ClientCertificateFile and ClientKeyFile must be
// provided together.
type HTTPClientConfig struct {
	BasicUsername         string
	BasicPassword         string
	BearerToken           string
	CACertificateFile     string
	ClientCertificateFile string
	ClientKeyFile         string
	InsecureSkipVerify    bool
}

// RequestError preserves structured transport and HTTP response information so
// the TUI can present a short operational message without discarding details.
type RequestError struct {
	Service    string
	Method     string
	Path       string
	StatusCode int
	Status     string
	Body       string
	Err        error
}

func (requestError *RequestError) Error() string {
	prefix := strings.TrimSpace(strings.Join([]string{requestError.Service, requestError.Method, requestError.Path}, " "))
	if requestError.StatusCode != 0 {
		message := strings.TrimSpace(requestError.Status)
		if requestError.Body != "" {
			message += ": " + strings.TrimSpace(requestError.Body)
		}
		if requestError.Err != nil {
			if message != "" {
				message += "; "
			}
			message += requestError.Err.Error()
		}
		return prefix + ": " + message
	}
	if requestError.Err != nil {
		return prefix + ": " + requestError.Err.Error()
	}
	return prefix
}

func (requestError *RequestError) Unwrap() error { return requestError.Err }

func newResponseError(response *http.Response, service, method, path string, bodyLimit int64) *RequestError {
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, bodyLimit))
	if readErr != nil {
		readErr = fmt.Errorf("read error response: %w", readErr)
	}
	return &RequestError{
		Service: service, Method: method, Path: path,
		StatusCode: response.StatusCode, Status: response.Status,
		Body: strings.TrimSpace(string(payload)), Err: readErr,
	}
}

// normalizeEndpoint accepts an optional reverse-proxy path while keeping
// credentials out of endpoint labels and request error messages.
func normalizeEndpoint(endpoint, service string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" || parsed.Opaque != "" {
		// Do not echo a malformed URL: it may contain credentials that could
		// not be parsed and redacted safely.
		return "", fmt.Errorf("invalid %s endpoint; use an HTTP or HTTPS URL", service)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("invalid %s endpoint scheme %q; use http or https", service, parsed.Scheme)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%s endpoint must not contain credentials; use the authentication options", service)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(endpoint, "#") {
		return "", fmt.Errorf("%s endpoint must not contain a query string or fragment", service)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func newHTTPClient(timeout time.Duration, config HTTPClientConfig) (*http.Client, error) {
	username := strings.TrimSpace(config.BasicUsername)
	token := strings.TrimSpace(config.BearerToken)
	if token != "" && (username != "" || config.BasicPassword != "") {
		return nil, errors.New("basic authentication and bearer authentication cannot be used together")
	}
	if config.BasicPassword != "" && username == "" {
		return nil, errors.New("basic authentication password requires a username")
	}
	if (config.ClientCertificateFile == "") != (config.ClientKeyFile == "") {
		return nil, errors.New("client certificate and client key must be configured together")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify, //nolint:gosec // Explicit operator-controlled diagnostic escape hatch.
		MinVersion:         tls.VersionTLS12,
	}
	if config.CACertificateFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		contents, err := os.ReadFile(config.CACertificateFile)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate %q: %w", config.CACertificateFile, err)
		}
		if !roots.AppendCertsFromPEM(contents) {
			return nil, fmt.Errorf("CA certificate %q contains no PEM certificates", config.CACertificateFile)
		}
		tlsConfig.RootCAs = roots
	}
	if config.ClientCertificateFile != "" {
		certificate, err := tls.LoadX509KeyPair(config.ClientCertificateFile, config.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate and key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	transport.TLSClientConfig = tlsConfig

	var roundTripper http.RoundTripper = transport
	if token != "" || username != "" {
		roundTripper = authenticatedTransport{
			base:        transport,
			username:    username,
			password:    config.BasicPassword,
			bearerToken: token,
		}
	}
	return &http.Client{
		Transport: roundTripper,
		Timeout:   timeout,
		CheckRedirect: func(request *http.Request, previous []*http.Request) error {
			if len(previous) == 0 {
				return nil
			}
			origin := previous[0].URL
			if !strings.EqualFold(request.URL.Scheme, origin.Scheme) || !strings.EqualFold(request.URL.Host, origin.Host) {
				// Never replay configured credentials to another origin or across
				// an HTTPS-to-HTTP downgrade. The caller receives the 3xx response.
				return http.ErrUseLastResponse
			}
			if len(previous) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
	}, nil
}

type authenticatedTransport struct {
	base        http.RoundTripper
	username    string
	password    string
	bearerToken string
}

func (transport authenticatedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	if transport.bearerToken != "" {
		clone.Header.Set("Authorization", "Bearer "+transport.bearerToken)
	} else {
		clone.SetBasicAuth(transport.username, transport.password)
	}
	return transport.base.RoundTrip(clone)
}
