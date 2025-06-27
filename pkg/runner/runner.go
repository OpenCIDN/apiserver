package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"github.com/OpenCIDN/apiserver/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/apiserver/pkg/clientset/versioned"
	"github.com/OpenCIDN/apiserver/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/apiserver/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/apiserver/pkg/versions"
	"github.com/wzshiming/ioswmr"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Runner executes Sync tasks
type Runner struct {
	handlerName           string
	client                versioned.Interface
	sharedInformerFactory externalversions.SharedInformerFactory
	syncInformer          informers.SyncInformer
	httpClient            *http.Client
	updateCh              chan struct{}
}

// NewRunner creates a new Runner instance
func NewRunner(handlerName string, clientset versioned.Interface) *Runner {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(clientset, 0)
	r := &Runner{
		handlerName:           handlerName,
		client:                clientset,
		sharedInformerFactory: sharedInformerFactory,
		syncInformer:          sharedInformerFactory.Task().V1alpha1().Syncs(),
		httpClient:            http.DefaultClient,
		updateCh:              make(chan struct{}, 1),
	}

	return r
}

// Run starts the runner
func (r *Runner) Run(ctx context.Context) error {

	r.syncInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			select {
			case r.updateCh <- struct{}{}:
			default:
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			select {
			case r.updateCh <- struct{}{}:
			default:
			}
		},
	})

	r.sharedInformerFactory.Start(ctx.Done())
	return r.worker(ctx)
}

func (r *Runner) worker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.updateCh:
			ctx := context.Background()
			// Get a pending sync to process
			sync, err := r.getPending(ctx)
			if err != nil {
				klog.Errorf("Error getting pending sync: %v", err)
				continue
			}

			sync.Status.Phase = v1alpha1.SyncPhaseRunning
			sync, err = r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("Error updating sync to running state: %v", err)
				continue
			}

			// Process the sync
			err = r.processSync(ctx, sync)
			if err != nil {
				sync.Status.Phase = v1alpha1.SyncPhaseFailed
				sync.Status.Conditions = append(sync.Status.Conditions, v1alpha1.Condition{
					Type:               "Process",
					Status:             v1alpha1.ConditionTrue,
					Reason:             "ProcessFailed",
					Message:            err.Error(),
					LastTransitionTime: metav1.Now(),
				})
				_, err = r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
				if err != nil {
					klog.Errorf("Error updating sync to failed state: %v", err)
				}
				continue
			}

			sync.Status.Progress = sync.Spec.Total
			sync.Status.Phase = v1alpha1.SyncPhaseSucceeded
			_, err = r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("Error updating sync to succeeded state: %v", err)
				continue
			}
		}
	}
}

// buildRequest constructs an HTTP request from SyncHTTP configuration
func (r *Runner) buildRequest(syncHTTP *v1alpha1.SyncHTTP, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(syncHTTP.Request.Method, syncHTTP.Request.URL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Set default headers
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", versions.DefaultUserAgent())

	// Add custom headers from configuration
	for k, v := range syncHTTP.Request.Headers {
		req.Header.Set(k, v)
	}

	if body != nil && req.ContentLength == 0 {
		if cl := req.Header.Get("Content-Length"); cl != "" {
			contentLength, err := strconv.ParseInt(cl, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid Content-Length header: %v", err)
			}
			req.ContentLength = contentLength
		}
	}

	return req, nil
}

func (r *Runner) processSync(ctx context.Context, sync *v1alpha1.Sync) error {

	srcReq, err := r.buildRequest(&sync.Spec.Source, nil)
	if err != nil {
		return err
	}

	srcResp, err := r.httpClient.Do(srcReq)
	if err != nil {
		return err
	}

	defer srcResp.Body.Close()

	if sync.Spec.Source.Response.StatusCode != 0 {
		if srcResp.StatusCode != sync.Spec.Source.Response.StatusCode {
			return fmt.Errorf("unexpected status code from source: got %d, want %d",
				srcResp.StatusCode, sync.Spec.Source.Response.StatusCode)
		}
	} else {
		if srcResp.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("source returned error status code: %d", srcResp.StatusCode)
		}
	}

	if srcResp.ContentLength != sync.Spec.Total {
		return fmt.Errorf("content length mismatch: got %d, want %d", srcResp.ContentLength, sync.Spec.Total)
	}

	swmr := ioswmr.NewSWMR(nil)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		_, err := io.Copy(swmr, srcResp.Body)
		return err
	})

	for _, dest := range sync.Spec.Destination {
		dest := dest // create local copy for goroutine
		g.Go(func() error {
			destReq, err := r.buildRequest(&dest, swmr.NewReader())
			if err != nil {
				return err
			}

			destResp, err := r.httpClient.Do(destReq)
			if err != nil {
				return err
			}
			defer destResp.Body.Close()

			if dest.Response.StatusCode != 0 {
				if destResp.StatusCode != dest.Response.StatusCode {
					return fmt.Errorf("unexpected status code from destination: got %d, want %d",
						destResp.StatusCode, dest.Response.StatusCode)
				}
			} else {
				if destResp.StatusCode >= http.StatusMultipleChoices {
					body, err := io.ReadAll(destResp.Body)
					if err != nil {
						return fmt.Errorf("destination returned error status code: %d (failed to read response body: %v)", destResp.StatusCode, err)
					}
					return fmt.Errorf("destination returned error status code: %d, body: %s", destResp.StatusCode, string(body))
				}
			}
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Runner) getPending(ctx context.Context) (*v1alpha1.Sync, error) {
	syncs, err := r.getPendingList(nil)
	if err != nil {
		return nil, err
	}

	for _, sync := range syncs {
		// Try to acquire the sync by setting handler name
		syncCopy := sync.DeepCopy()
		syncCopy.Spec.HandlerName = r.handlerName

		updatedSync, err := r.client.TaskV1alpha1().Syncs().Update(ctx, syncCopy, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) {
				// Someone else got the sync first, try next one
				continue
			}
			return nil, err
		}

		// Successfully acquired the sync
		return updatedSync, nil
	}

	// No pending syncs available
	return nil, fmt.Errorf("no pending syncs available")
}

// getPendingList returns all Syncs in Pending state, sorted by weight and creation time
func (r *Runner) getPendingList(syncs []*v1alpha1.Sync) ([]*v1alpha1.Sync, error) {
	syncs, err := r.syncInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}

	if len(syncs) == 0 {
		return nil, nil
	}

	var pendingSyncs []*v1alpha1.Sync

	// Filter for Pending state
	for _, sync := range syncs {
		if sync.Spec.HandlerName == "" && sync.Status.Phase == v1alpha1.SyncPhasePending {
			pendingSyncs = append(pendingSyncs, sync)
		}
	}

	// Sort by weight (descending) and creation time (ascending)
	sort.Slice(pendingSyncs, func(i, j int) bool {
		if pendingSyncs[i].Spec.Weight != pendingSyncs[j].Spec.Weight {
			return pendingSyncs[i].Spec.Weight > pendingSyncs[j].Spec.Weight
		}
		return pendingSyncs[i].CreationTimestamp.Before(&pendingSyncs[j].CreationTimestamp)
	})

	return pendingSyncs, nil
}

// writerCounter implements io.Writer to count bytes written in a thread-safe manner
type writerCounter struct {
	count int64
	mu    sync.Mutex
}

func (w *writerCounter) Write(p []byte) (int, error) {
	n := len(p)
	w.mu.Lock()
	w.count += int64(n)
	w.mu.Unlock()
	return n, nil
}

func (w *writerCounter) Count() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}
