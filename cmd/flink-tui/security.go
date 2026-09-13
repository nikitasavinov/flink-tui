package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

type securityFlags struct {
	username            *string
	passwordFile        *string
	bearerTokenFile     *string
	caCertificateFile   *string
	clientCertificate   *string
	clientKey           *string
	insecureSkipVerify  *bool
	passwordEnvironment string
	tokenEnvironment    string
}

func registerSecurityFlags(flags *flag.FlagSet, prefix, environmentPrefix, label string) securityFlags {
	flagPrefix := prefix
	if flagPrefix != "" {
		flagPrefix += "-"
	}
	return securityFlags{
		username:            flags.String(flagPrefix+"username", os.Getenv(environmentPrefix+"USERNAME"), label+" basic-auth username"),
		passwordFile:        flags.String(flagPrefix+"password-file", "", label+" basic-auth password file (or "+environmentPrefix+"PASSWORD)"),
		bearerTokenFile:     flags.String(flagPrefix+"bearer-token-file", "", label+" bearer-token file (or "+environmentPrefix+"BEARER_TOKEN)"),
		caCertificateFile:   flags.String(flagPrefix+"ca-cert", os.Getenv(environmentPrefix+"CA_CERT"), label+" PEM CA bundle"),
		clientCertificate:   flags.String(flagPrefix+"client-cert", os.Getenv(environmentPrefix+"CLIENT_CERT"), label+" mTLS client certificate"),
		clientKey:           flags.String(flagPrefix+"client-key", os.Getenv(environmentPrefix+"CLIENT_KEY"), label+" mTLS client key"),
		insecureSkipVerify:  flags.Bool(flagPrefix+"insecure-skip-verify", false, "skip "+label+" TLS verification (unsafe)"),
		passwordEnvironment: environmentPrefix + "PASSWORD",
		tokenEnvironment:    environmentPrefix + "BEARER_TOKEN",
	}
}

func (flags securityFlags) config() (flink.HTTPClientConfig, error) {
	password, err := secretFromFileOrEnvironment(*flags.passwordFile, flags.passwordEnvironment)
	if err != nil {
		return flink.HTTPClientConfig{}, fmt.Errorf("basic-auth password: %w", err)
	}
	token, err := secretFromFileOrEnvironment(*flags.bearerTokenFile, flags.tokenEnvironment)
	if err != nil {
		return flink.HTTPClientConfig{}, fmt.Errorf("bearer token: %w", err)
	}
	return flink.HTTPClientConfig{
		BasicUsername:         *flags.username,
		BasicPassword:         password,
		BearerToken:           token,
		CACertificateFile:     *flags.caCertificateFile,
		ClientCertificateFile: *flags.clientCertificate,
		ClientKeyFile:         *flags.clientKey,
		InsecureSkipVerify:    *flags.insecureSkipVerify,
	}, nil
}

func secretFromFileOrEnvironment(path, environment string) (string, error) {
	if path == "" {
		return os.Getenv(environment), nil
	}
	contents, err := os.ReadFile(path) //nolint:gosec // The operator explicitly supplies the credential file path.
	if err != nil {
		return "", fmt.Errorf("read %q: %w", path, err)
	}
	return strings.TrimRight(string(contents), "\r\n"), nil
}
