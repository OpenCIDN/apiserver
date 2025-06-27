package controller

import (
	"context"

	"github.com/OpenCIDN/apiserver/pkg/clientset/versioned"
	"github.com/OpenCIDN/apiserver/pkg/informers/externalversions"
	"github.com/wzshiming/sss"
)

type ControllerManager struct {
	blobController        *BlobController
	sharedInformerFactory externalversions.SharedInformerFactory
}

func NewControllerManager(client *versioned.Clientset, s3 *sss.SSS) (*ControllerManager, error) {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)

	blobController := NewBlobController(
		s3,
		sharedInformerFactory,
		client,
	)
	return &ControllerManager{
		sharedInformerFactory: sharedInformerFactory,
		blobController:        blobController,
	}, nil
}

func (m *ControllerManager) Run(ctx context.Context) error {
	err := m.blobController.Run(ctx)
	if err != nil {
		return err
	}

	m.sharedInformerFactory.Start(ctx.Done())
	return nil
}
