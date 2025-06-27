/*
Copyright 2025 The OpenCIDN Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"

	myapiserver "github.com/OpenCIDN/apiserver/pkg/apiserver"
	generatedopenapi "github.com/OpenCIDN/apiserver/pkg/openapi"
	"github.com/spf13/cobra"
	"k8s.io/api/node/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	utilcompatibility "k8s.io/apiserver/pkg/util/compatibility"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/term"
	"k8s.io/klog/v2"
)

const defaultEtcdPathPrefix = "/registry/opencidn.daocloud.io"

type Options struct {
	SecureServing *genericoptions.SecureServingOptionsWithLoopback

	Etcd *genericoptions.EtcdOptions
}

func (o *Options) Flags() (fs cliflag.NamedFlagSets) {

	o.SecureServing.AddFlags(fs.FlagSet("apiserver secure serving"))

	o.Etcd.AddFlags(fs.FlagSet("Etcd"))

	return fs
}

// Validate validates ServerOptions
func (o Options) Validate(args []string) error {

	errs := o.Etcd.Validate()

	return utilerrors.NewAggregate(errs)
}

type ServerConfig struct {
	Apiserver *genericapiserver.Config
	Rest      *rest.Config
}

func (o Options) ServerConfig() (*myapiserver.Config, error) {
	apiservercfg, err := o.ApiserverConfig()
	if err != nil {
		return nil, err
	}

	storageConfigCopy := o.Etcd.StorageConfig
	if storageConfigCopy.StorageObjectCountTracker == nil {
		storageConfigCopy.StorageObjectCountTracker = apiservercfg.StorageObjectCountTracker
	}

	if o.Etcd.ApplyWithStorageFactoryTo(serverstorage.NewDefaultStorageFactory(
		o.Etcd.StorageConfig,
		o.Etcd.DefaultStorageMediaType,
		myapiserver.Codecs,
		serverstorage.NewDefaultResourceEncodingConfig(myapiserver.Scheme),
		apiservercfg.MergedResourceConfig,
		nil), &apiservercfg.Config); err != nil {
		return nil, err
	}

	return &myapiserver.Config{
		GenericConfig: apiservercfg,
	}, nil
}

func (o Options) ApiserverConfig() (*genericapiserver.RecommendedConfig, error) {
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	serverConfig := genericapiserver.NewRecommendedConfig(myapiserver.Codecs)
	if err := o.SecureServing.ApplyTo(&serverConfig.SecureServing, &serverConfig.LoopbackClientConfig); err != nil {
		return nil, err
	}

	// enable OpenAPI schemas
	namer := openapinamer.NewDefinitionNamer(myapiserver.Scheme)
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(generatedopenapi.GetOpenAPIDefinitions, namer)
	serverConfig.OpenAPIConfig.Info.Title = "OpenCIDN"
	serverConfig.OpenAPIConfig.Info.Version = "0.1"

	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(generatedopenapi.GetOpenAPIDefinitions, namer)
	serverConfig.OpenAPIV3Config.Info.Title = "OpenCIDN"
	serverConfig.OpenAPIV3Config.Info.Version = "0.1"

	serverConfig.EffectiveVersion = utilcompatibility.DefaultBuildEffectiveVersion()

	return serverConfig, nil
}

// NewHelloServerCommand provides a CLI handler for the metrics server entrypoint
func NewHelloServerCommand(stopCh <-chan struct{}) *cobra.Command {
	opts := &Options{
		SecureServing: genericoptions.NewSecureServingOptions().WithLoopback(),
		Etcd:          genericoptions.NewEtcdOptions(storagebackend.NewDefaultConfig(defaultEtcdPathPrefix, nil)),
	}
	opts.Etcd.StorageConfig.EncodeVersioner = runtime.NewMultiGroupVersioner(v1alpha1.SchemeGroupVersion, schema.GroupKind{Group: v1alpha1.GroupName})
	opts.SecureServing.BindPort = 6443

	cmd := &cobra.Command{
		Short: "Launch",
		RunE: func(c *cobra.Command, args []string) error {
			if err := opts.Validate(args); err != nil {
				return err
			}
			if err := runCommand(c.Context(), opts); err != nil {
				return err
			}
			return nil
		},
	}

	fs := cmd.Flags()
	nfs := opts.Flags()
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}
	local := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	klog.InitFlags(local)
	nfs.FlagSet("logging").AddGoFlagSet(local)

	usageFmt := "Usage:\n  %s\n"
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStderr(), nfs, cols)
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStdout(), nfs, cols)
	})
	return cmd
}

func runCommand(ctx context.Context, o *Options) error {
	servercfg, err := o.ServerConfig()
	if err != nil {
		return err
	}

	server, err := servercfg.Complete().New()
	if err != nil {
		return err
	}

	return server.PrepareRun().RunWithContext(ctx)
}
