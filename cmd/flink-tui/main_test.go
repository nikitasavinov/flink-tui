package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui"
)

func TestParseCLIOptionsWiresFlagsAndSecurityEnvironment(t *testing.T) {
	clearSecurityEnvironment(t)
	t.Setenv("FLINK_TUI_PASSWORD", "cluster-password")
	t.Setenv("FLINK_TUI_CA_CERT", "cluster-ca.pem")
	t.Setenv("FLINK_TUI_SQL_USERNAME", "environment-sql-user")

	var output bytes.Buffer
	options, err := parseCLIOptions([]string{
		"-endpoint", "https://flink.example",
		"-sql-endpoint", "https://sql.example",
		"-job", "job-123",
		"-refresh", "7s",
		"-username", "flag-cluster-user",
		"-insecure-skip-verify",
		"-sql-username", "flag-sql-user",
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if options.endpoint != "https://flink.example" || options.sqlEndpoint != "https://sql.example" ||
		options.jobID != "job-123" || options.refresh != 7*time.Second {
		t.Fatalf("parsed options = %#v", options)
	}

	flinkConfig, err := options.flinkSecurity.config()
	if err != nil {
		t.Fatal(err)
	}
	if flinkConfig.BasicUsername != "flag-cluster-user" || flinkConfig.BasicPassword != "cluster-password" ||
		flinkConfig.CACertificateFile != "cluster-ca.pem" || !flinkConfig.InsecureSkipVerify {
		t.Fatalf("Flink security config = %#v", flinkConfig)
	}
	sqlConfig, err := options.sqlSecurity.config()
	if err != nil {
		t.Fatal(err)
	}
	if sqlConfig.BasicUsername != "flag-sql-user" {
		t.Fatalf("SQL username = %q, want flag-sql-user", sqlConfig.BasicUsername)
	}
}

func TestRunCLIStartsConfiguredModelWithAndWithoutSQL(t *testing.T) {
	clearSecurityEnvironment(t)
	for _, sqlEndpoint := range []string{"", "http://localhost:8083"} {
		t.Run("sql="+sqlEndpoint, func(t *testing.T) {
			var stderr bytes.Buffer
			calls := 0
			exitCode := runCLI([]string{
				"-endpoint", "localhost:8081",
				"-sql-endpoint", sqlEndpoint,
				"-job", "job-123",
				"-refresh", "5s",
			}, &stderr, func(model tea.Model) error {
				calls++
				if _, ok := model.(ui.Model); !ok {
					t.Fatalf("runner model type = %T, want ui.Model", model)
				}
				return nil
			})
			if exitCode != 0 || calls != 1 || stderr.Len() != 0 {
				t.Fatalf("run = exit:%d calls:%d stderr:%q", exitCode, calls, stderr.String())
			}
		})
	}
}

func TestRunCLIReportsConfigurationAndProgramErrors(t *testing.T) {
	clearSecurityEnvironment(t)
	missing := t.TempDir() + "/missing"
	tests := []struct {
		name     string
		args     []string
		runError error
		wantCode int
		wantText string
	}{
		{name: "invalid flag", args: []string{"-refresh", "later"}, wantCode: 2, wantText: "invalid value"},
		{name: "zero refresh", args: []string{"-refresh", "0"}, wantCode: 2, wantText: "at least 250ms"},
		{name: "negative refresh", args: []string{"-refresh", "-1s"}, wantCode: 2, wantText: "at least 250ms"},
		{name: "too-frequent refresh", args: []string{"-refresh", "249ms"}, wantCode: 2, wantText: "at least 250ms"},
		{name: "Flink secret", args: []string{"-password-file", missing}, wantCode: 2, wantText: "basic-auth password"},
		{name: "Flink endpoint", args: []string{"-endpoint", "ftp://flink", "-sql-endpoint", ""}, wantCode: 2, wantText: "invalid Flink endpoint scheme"},
		{name: "SQL secret", args: []string{"-sql-bearer-token-file", missing}, wantCode: 2, wantText: "bearer token"},
		{name: "SQL endpoint", args: []string{"-sql-endpoint", "ftp://sql"}, wantCode: 2, wantText: "invalid SQL Gateway endpoint scheme"},
		{name: "program", args: []string{"-sql-endpoint", ""}, runError: errors.New("terminal failed"), wantCode: 1, wantText: "terminal failed"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			exitCode := runCLI(test.args, &stderr, func(tea.Model) error { return test.runError })
			if exitCode != test.wantCode {
				t.Fatalf("exit code = %d, want %d; stderr=%q", exitCode, test.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), test.wantText) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), test.wantText)
			}
		})
	}
}

func TestRunCLIHelpExitsSuccessfullyWithoutStartingProgram(t *testing.T) {
	clearSecurityEnvironment(t)
	var output bytes.Buffer
	called := false
	exitCode := runCLI([]string{"-help"}, &output, func(tea.Model) error {
		called = true
		return nil
	})
	if exitCode != 0 || called || !strings.Contains(output.String(), "Usage of flink-tui") {
		t.Fatalf("help = exit:%d called:%t output:%q", exitCode, called, output.String())
	}
}

func TestRunCLIRejectsPositionalArgumentsWithoutStartingProgram(t *testing.T) {
	clearSecurityEnvironment(t)
	for _, args := range [][]string{
		{"https://flink.example"},
		{"https://flink.example", "--endpoint", "https://another.example"},
		{"--endpoint", "https://flink.example", "extra"},
		{"--", "--endpoint", "https://flink.example"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var output bytes.Buffer
			called := false
			exitCode := runCLI(args, &output, func(tea.Model) error {
				called = true
				return nil
			})
			if exitCode != 2 || called || !strings.Contains(output.String(), "unexpected positional argument") {
				t.Fatalf("run = exit:%d called:%t output:%q", exitCode, called, output.String())
			}
		})
	}
}

func clearSecurityEnvironment(t *testing.T) {
	t.Helper()
	for _, prefix := range []string{"FLINK_TUI_", "FLINK_TUI_SQL_"} {
		for _, suffix := range []string{"USERNAME", "PASSWORD", "BEARER_TOKEN", "CA_CERT", "CLIENT_CERT", "CLIENT_KEY"} {
			t.Setenv(prefix+suffix, "")
		}
	}
}
