package controller

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/OpenCIDN/apiserver/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/apiserver/pkg/clientset/versioned"
	"github.com/wzshiming/sss"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/OpenCIDN/apiserver/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/apiserver/pkg/informers/externalversions/task/v1alpha1"
)

type BlobController struct {
	s3 *sss.SSS

	expires time.Duration

	httpClient *http.Client

	client       versioned.Interface
	blobInformer informers.BlobInformer
	syncInformer informers.SyncInformer

	pendingSize int64
	runningSize int64
}

func NewBlobController(
	s3 *sss.SSS,
	sharedInformerFactory externalversions.SharedInformerFactory,
	client versioned.Interface,
) *BlobController {
	return &BlobController{
		s3:           s3,
		expires:      time.Hour,
		httpClient:   http.DefaultClient,
		blobInformer: sharedInformerFactory.Task().V1alpha1().Blobs(),
		syncInformer: sharedInformerFactory.Task().V1alpha1().Syncs(),
		client:       client,
		pendingSize:  2,
		runningSize:  3,
	}
}

func (c *BlobController) Run(ctx context.Context) error {
	c.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			blob := obj.(*v1alpha1.Blob)
			if blob.Spec.Total == 0 {
				fi, err := httpStat(blob.Spec.Source, c.httpClient, nil)
				if err != nil {
					klog.Errorf("failed to get file info for blob %s: %v", blob.Name, err)
					return
				}

				blob.Spec.Total = fi.Size
				if !fi.Range {
					blob.Spec.ChunkSize = fi.Size
				}

				blob, err = c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
				if err != nil {
					klog.Errorf("failed to update blob %s status: %v", blob.Name, err)
					return
				}
			}

			if blob.Spec.ChunkSize != 0 && blob.Spec.Total > blob.Spec.ChunkSize {
				klog.Infof("Creating multiple syncs for blob %s with total size %d and chunk size %d", blob.Name, blob.Spec.Total, blob.Spec.ChunkSize)
				err := c.createSyncs(ctx, blob)
				if err != nil {
					klog.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
					return
				}
			} else {
				klog.Infof("Creating single sync for blob %s with total size %d", blob.Name, blob.Spec.Total)
				err := c.createOneSync(ctx, blob)
				if err != nil {
					klog.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
					return
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
		},
		DeleteFunc: func(obj interface{}) {
			// blob, ok := obj.(*v1alpha1.Blob)
			// if !ok {
			// 	return
			// }
			// err := c.client.TaskV1alpha1().Syncs().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
			// 	LabelSelector: labels.Set{BlobNameLabel: blob.Name}.String(),
			// })
			// if err != nil {
			// 	klog.Errorf("failed to delete syncs for blob %s: %v", blob.Name, err)
			// }
		},
	})

	c.syncInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newSync := newObj.(*v1alpha1.Sync)
			oldSync := oldObj.(*v1alpha1.Sync)

			blobName := newSync.Labels[BlobNameLabel]
			if blobName == "" {
				return
			}

			if newSync.Status.Phase == oldSync.Status.Phase {
				return
			}

			if reflect.DeepEqual(newSync.Status, oldSync.Status) {
				return
			}

			blob, err := c.blobInformer.Lister().Get(blobName)
			if err != nil {
				klog.Errorf("failed to get blob %s: %v", blobName, err)
				return
			}

			if blob.Spec.ChunkSize != 0 && blob.Spec.Total > blob.Spec.ChunkSize {
				err = c.createSyncs(ctx, blob)
				if err != nil {
					klog.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
					return
				}
			}

			err = c.updateBlobStatusFromSyncs(ctx, blob)
			if err != nil {
				klog.Errorf("failed to update blob %s status: %v", blob.Name, err)
				return
			}
		},
	})
	return nil
}

func (c *BlobController) createOneSync(ctx context.Context, blob *v1alpha1.Blob) error {
	sync := &v1alpha1.Sync{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%d-%d", blob.Name, 0, blob.Spec.Total),
			Labels: map[string]string{
				BlobNameLabel: blob.Name,
			},
		},
		Spec: v1alpha1.SyncSpec{
			Total:  blob.Spec.Total,
			Weight: blob.Spec.Weight,
		},
		Status: v1alpha1.SyncStatus{
			Phase: v1alpha1.SyncPhasePending,
		},
	}

	sync.Spec.Source = v1alpha1.SyncHTTP{
		Request: v1alpha1.SyncHTTPRequest{
			Method: http.MethodGet,
			URL:    blob.Spec.Source,
		},
	}
	for _, dst := range blob.Spec.Destination {
		d, err := c.s3.SignPut(dst, c.expires)
		if err != nil {
			return err
		}

		sync.Spec.Destination = append(sync.Spec.Destination, v1alpha1.SyncHTTP{
			Request: v1alpha1.SyncHTTPRequest{
				Method: http.MethodPut,
				URL:    d,
				Headers: map[string]string{
					"Content-Length": fmt.Sprintf("%d", blob.Spec.Total),
				},
			},
		})
	}

	_, err := c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// getChunkCount returns the number of chunks needed for the blob based on total size and chunk size
func (c *BlobController) getChunkCount(blob *v1alpha1.Blob) int64 {
	total := blob.Spec.Total
	chunkSize := blob.Spec.ChunkSize

	count := total / chunkSize
	if total%chunkSize != 0 {
		count++
	}
	return count
}

const BlobNameLabel = v1alpha1.GroupName + "/blob-name"

func (c *BlobController) createSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
	chunks := c.getChunkCount(blob)

	uploadIDs := blob.Status.UploadIDs
	if len(uploadIDs) == 0 {
		for _, dst := range blob.Spec.Destination {
			mp, err := c.s3.NewMultipart(ctx, dst)
			if err != nil {
				return err
			}
			uploadIDs = append(uploadIDs, mp.UploadID())
		}

		blob.Status.UploadIDs = uploadIDs
		newBlob, err := c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		blob = newBlob
	}

	// Get current syncs from informer cache
	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobNameLabel: blob.Name,
	}))
	if err != nil {
		return err
	}

	// Count pending and running syncs
	pendingCount := 0
	runningCount := 0
	failedCount := 0
	for _, sync := range syncs {
		switch sync.Status.Phase {
		case v1alpha1.SyncPhasePending:
			pendingCount++
		case v1alpha1.SyncPhaseRunning:
			runningCount++
		case v1alpha1.SyncPhaseFailed:
			failedCount++
		}
	}

	// Only create new syncs if we have room
	if failedCount != 0 || pendingCount >= int(c.pendingSize) || runningCount >= int(c.runningSize) {
		return nil
	}

	// Calculate how many new syncs we can create
	toCreate := int(c.pendingSize) - pendingCount
	if toCreate <= 0 {
		return nil
	}

	created := 0
	for i := int64(0); i < int64(chunks) && created < toCreate; i++ {
		start := i * blob.Spec.ChunkSize
		end := start + blob.Spec.ChunkSize
		if end > blob.Spec.Total {
			end = blob.Spec.Total
		}

		name := fmt.Sprintf("%s-%d-%d", blob.Name, start, end)

		_, err := c.syncInformer.Lister().Get(name)
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			return err
		}

		sync := &v1alpha1.Sync{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					BlobNameLabel: blob.Name,
				},
			},
			Spec: v1alpha1.SyncSpec{
				Total:  end - start,
				Weight: blob.Spec.Weight,
			},
			Status: v1alpha1.SyncStatus{
				Phase: v1alpha1.SyncPhasePending,
			},
		}

		sync.Spec.Source = v1alpha1.SyncHTTP{
			Request: v1alpha1.SyncHTTPRequest{
				Method: http.MethodGet,
				URL:    blob.Spec.Source,
				Headers: map[string]string{
					"Range": fmt.Sprintf("bytes=%d-%d", start, end-1),
				},
			},
		}

		for i, dst := range blob.Spec.Destination {
			mp := c.s3.GetMultipartWithUploadID(dst, blob.Status.UploadIDs[i])

			partURL, err := mp.SignUploadPart(int64(i+1), c.expires)
			if err != nil {
				return err
			}

			sync.Spec.Destination = append(sync.Spec.Destination, v1alpha1.SyncHTTP{
				Request: v1alpha1.SyncHTTPRequest{
					Method: http.MethodPut,
					URL:    partURL,
					Headers: map[string]string{
						"Content-Length": fmt.Sprintf("%d", end-start),
					},
				},
			})
		}

		_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		created++
	}

	return nil
}

func (c *BlobController) updateBlobStatusFromSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobNameLabel: blob.Name,
	}))
	if err != nil {
		return fmt.Errorf("failed to list syncs: %w", err)
	}

	chunks := c.getChunkCount(blob)
	var succeeded, failed, pending int64
	var progress int64
	for _, sync := range syncs {
		switch sync.Status.Phase {
		case v1alpha1.SyncPhaseSucceeded:
			succeeded++
		case v1alpha1.SyncPhaseFailed:
			failed++
		case v1alpha1.SyncPhasePending:
			pending++
		}
		progress += sync.Status.Progress
	}

	blob.Status.Progress = progress

	if chunks == succeeded {
		blob.Status.Phase = v1alpha1.BlobPhaseSucceeded
	} else if failed == 0 {
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
	} else {
		blob.Status.Phase = v1alpha1.BlobPhaseFailed
	}

	_, err = c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update blob status: %w", err)
	}

	if chunks <= 1 || chunks != succeeded {
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)
	for i, dst := range blob.Spec.Destination {
		uploadID := blob.Status.UploadIDs[i]
		dst := dst
		g.Go(func() error {
			mp := c.s3.GetMultipartWithUploadID(dst, uploadID)
			return mp.Commit(ctx)
		})
	}
	return g.Wait()
}
