package ui

import (
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/coordinator"
)

// Model is the application coordinator exposed through the stable UI facade.
type Model = coordinator.Model

// NewModel creates a TUI backed by the Flink REST API.
func NewModel(client *flink.Client, preferredJobID string, refreshInterval time.Duration) Model {
	return coordinator.NewModel(client, preferredJobID, refreshInterval)
}

// NewModelWithSQLGateway creates a TUI with optional SQL Gateway integration.
func NewModelWithSQLGateway(
	client *flink.Client,
	sqlClient *flink.SQLGatewayClient,
	preferredJobID string,
	refreshInterval time.Duration,
) Model {
	return coordinator.NewModelWithSQLGateway(client, sqlClient, preferredJobID, refreshInterval)
}
