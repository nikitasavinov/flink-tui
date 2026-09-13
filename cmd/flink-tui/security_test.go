package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSecurityConfigReadsSecretsFromEnvironmentAndFiles(t *testing.T) {
	t.Setenv("TEST_FLINK_PASSWORD", "environment-password")
	t.Setenv("TEST_FLINK_TOKEN", "environment-token")
	empty := ""
	username := "operator"
	caPath := "cluster-ca.pem"
	clientCertificate := "client.pem"
	clientKey := "client-key.pem"
	insecure := true
	flags := securityFlags{
		username:            &username,
		passwordFile:        &empty,
		bearerTokenFile:     &empty,
		caCertificateFile:   &caPath,
		clientCertificate:   &clientCertificate,
		clientKey:           &clientKey,
		insecureSkipVerify:  &insecure,
		passwordEnvironment: "TEST_FLINK_PASSWORD",
		tokenEnvironment:    "TEST_FLINK_TOKEN",
	}
	config, err := flags.config()
	if err != nil {
		t.Fatal(err)
	}
	if config.BasicUsername != username || config.BasicPassword != "environment-password" ||
		config.BearerToken != "environment-token" || config.CACertificateFile != caPath ||
		config.ClientCertificateFile != clientCertificate || config.ClientKeyFile != clientKey || !config.InsecureSkipVerify {
		t.Fatalf("environment config = %#v", config)
	}

	directory := t.TempDir()
	passwordPath := filepath.Join(directory, "password")
	tokenPath := filepath.Join(directory, "token")
	if err := os.WriteFile(passwordPath, []byte("file-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("file-token\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	flags.passwordFile = &passwordPath
	flags.bearerTokenFile = &tokenPath
	config, err = flags.config()
	if err != nil {
		t.Fatal(err)
	}
	if config.BasicPassword != "file-password" || config.BearerToken != "file-token" {
		t.Fatalf("file secrets = password %q token %q", config.BasicPassword, config.BearerToken)
	}
}

func TestSecurityConfigReportsUnreadableSecretFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	empty := ""
	insecure := false
	flags := securityFlags{
		username:            &empty,
		passwordFile:        &missing,
		bearerTokenFile:     &empty,
		caCertificateFile:   &empty,
		clientCertificate:   &empty,
		clientKey:           &empty,
		insecureSkipVerify:  &insecure,
		passwordEnvironment: "TEST_MISSING_PASSWORD",
		tokenEnvironment:    "TEST_MISSING_TOKEN",
	}
	if _, err := flags.config(); err == nil {
		t.Fatal("missing password file was accepted")
	}
}
