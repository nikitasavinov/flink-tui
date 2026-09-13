package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

type cliOptions struct {
	endpoint      string
	sqlEndpoint   string
	jobID         string
	refresh       time.Duration
	flinkSecurity securityFlags
	sqlSecurity   securityFlags
}

type programRunner func(tea.Model) error

const minimumRefreshInterval = 250 * time.Millisecond

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stderr, runProgram))
}

func parseCLIOptions(arguments []string, output io.Writer) (cliOptions, error) {
	flags := flag.NewFlagSet("flink-tui", flag.ContinueOnError)
	flags.SetOutput(output)
	endpoint := flags.String("endpoint", "http://localhost:8081", "Flink JobManager REST endpoint")
	sqlEndpoint := flags.String("sql-endpoint", "http://localhost:8083", "Flink SQL Gateway REST endpoint (empty disables SQL)")
	jobID := flags.String("job", "", "job ID to open directly (defaults to the cluster overview)")
	refresh := flags.Duration("refresh", 3*time.Second, "metrics refresh interval")
	flinkSecurity := registerSecurityFlags(flags, "", "FLINK_TUI_", "Flink REST")
	sqlSecurity := registerSecurityFlags(flags, "sql", "FLINK_TUI_SQL_", "SQL Gateway")
	if err := flags.Parse(arguments); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 0 {
		err := fmt.Errorf("unexpected positional argument %q; use --endpoint to select a cluster", flags.Arg(0))
		writeCLIError(output, err)
		return cliOptions{}, err
	}
	return cliOptions{
		endpoint: *endpoint, sqlEndpoint: *sqlEndpoint, jobID: *jobID, refresh: *refresh,
		flinkSecurity: flinkSecurity, sqlSecurity: sqlSecurity,
	}, nil
}

func runCLI(arguments []string, stderr io.Writer, run programRunner) int {
	options, err := parseCLIOptions(arguments, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		return 2
	}
	if err := validateCLIOptions(options); err != nil {
		writeCLIError(stderr, err)
		return 2
	}

	flinkConfig, err := options.flinkSecurity.config()
	if err != nil {
		writeCLIError(stderr, err)
		return 2
	}
	client, err := flink.NewClientWithConfig(options.endpoint, flinkConfig)
	if err != nil {
		writeCLIError(stderr, err)
		return 2
	}
	var sqlClient *flink.SQLGatewayClient
	if options.sqlEndpoint != "" {
		sqlConfig, configErr := options.sqlSecurity.config()
		if configErr != nil {
			writeCLIError(stderr, configErr)
			return 2
		}
		sqlClient, err = flink.NewSQLGatewayClientWithConfig(options.sqlEndpoint, sqlConfig)
		if err != nil {
			writeCLIError(stderr, err)
			return 2
		}
	}

	if err := run(ui.NewModelWithSQLGateway(client, sqlClient, options.jobID, options.refresh)); err != nil {
		writeCLIError(stderr, err)
		return 1
	}
	return 0
}

func validateCLIOptions(options cliOptions) error {
	if options.refresh < minimumRefreshInterval {
		return fmt.Errorf("refresh interval must be at least %s", minimumRefreshInterval)
	}
	return nil
}

func writeCLIError(writer io.Writer, err error) {
	_, _ = fmt.Fprintln(writer, "flink-tui:", err)
}

func runProgram(model tea.Model) error {
	shared.DetectTerminalBackground()
	_, err := tea.NewProgram(model).Run()
	return err
}
