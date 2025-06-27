package controllermanager

import (
	"context"
	"fmt"

	"github.com/OpenCIDN/apiserver/pkg/clientset/versioned"
	"github.com/OpenCIDN/apiserver/pkg/controller"
	"github.com/spf13/cobra"
	"github.com/wzshiming/sss"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type flagpole struct {
	Kubeconfig string
	Master     string
}

func NewControllerManagerCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Use:  "controller-manager",
		Long: `The OpenCIDN controller manager runs controllers that reconcile the desired state of the cluster.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var config *rest.Config
			var err error
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

			s3, err := sss.NewSSS(sss.WithURL("minio://minioadmin:minioadmin@myminio.us-east-1?forcepathstyle=true&secure=false&chunksize=104857600&regionendpoint=http://minio:9000"))
			if err != nil {
				return fmt.Errorf("error creating s3 client: %v", err)
			}

			ctr, err := controller.NewControllerManager(clientset, s3)
			if err != nil {
				return fmt.Errorf("error creating controller: %v", err)
			}

			err = ctr.Run(ctx)
			if err != nil {
				return fmt.Errorf("error running controller: %v", err)
			}

			<-ctx.Done()
			return nil
		},
	}

	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	return cmd
}
