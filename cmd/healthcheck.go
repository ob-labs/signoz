package cmd

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	var port int
	var ready bool
	command := &cobra.Command{
		Use:   "healthcheck",
		Short: "Check the local SigNoz HTTP server without starting services",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if port < 1 || port > 65535 {
				return fmt.Errorf("health check port must be between 1 and 65535")
			}

			client := &http.Client{
				Timeout: 2 * time.Second,
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
			path := "/api/v1/health"
			if ready {
				path = "/api/v1/ready"
			}
			url := "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + path
			request, err := http.NewRequestWithContext(command.Context(), http.MethodGet, url, nil)
			if err != nil {
				return err
			}
			response, err := client.Do(request)
			if err != nil {
				return fmt.Errorf("health check failed: %w", err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("health check failed: HTTP %d", response.StatusCode)
			}
			return nil
		},
	}
	command.Flags().IntVar(&port, "port", 8080, "local API server port (match apiserver.address when customized)")
	command.Flags().BoolVar(&ready, "ready", false, "check telemetry database readiness instead of local HTTP liveness")
	RootCmd.AddCommand(command)
}
