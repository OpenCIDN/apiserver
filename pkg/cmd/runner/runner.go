package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"context"

	"github.com/OpenCIDN/apiserver/pkg/clientset/versioned"
	"github.com/OpenCIDN/apiserver/pkg/runner"
	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type flagpole struct {
	Kubeconfig string
	Master     string
}

func NewRunnerCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Use:   "runner",
		Short: "Run the sync runner",
		RunE: func(cmd *cobra.Command, args []string) error {

			ident, err := identity()
			if err != nil {
				return fmt.Errorf("error getting identity: %v", err)
			}
			var config *rest.Config

			if flags.Kubeconfig != "" {
				config, err = clientcmd.BuildConfigFromFlags(flags.Master, flags.Kubeconfig)
			} else {
				config, err = rest.InClusterConfig()
			}

			if err != nil {
				return fmt.Errorf("error getting config: %v", err)
			}

			clientset, err := versioned.NewForConfig(config)
			if err != nil {
				return fmt.Errorf("error creating clientset: %v", err)
			}

			// Create runner
			runner := runner.NewRunner(ident, clientset)

			return runner.Run(ctx)
		},
	}

	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	return cmd
}

func identity() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("unable to get hostname: %w", err)
	}
	h := sha256.Sum256([]byte(hostname))
	hnHex := hex.EncodeToString(h[:])
	return fmt.Sprintf("%s-%d", hnHex[:16], time.Now().Unix()), nil
}
