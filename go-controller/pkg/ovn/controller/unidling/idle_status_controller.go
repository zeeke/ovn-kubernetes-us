package unidling

import (
	"fmt"
	"strings"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	kapi "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const OvnServiceIdledSuffix = "idled-at"

type idleStatusController struct {
	kube             kube.Interface
	gracePeriodQueue workqueue.DelayingInterface
	gracePeriod      time.Duration
	serviceLister    v1.ServiceLister
}

func NewIdleStatusController(k kube.Interface, serviceInformer coreinformers.ServiceInformer, gracePeriod time.Duration) *idleStatusController {

	isc := &idleStatusController{
		kube:             k,
		gracePeriodQueue: workqueue.NewDelayingQueue(),
		gracePeriod:      gracePeriod,
	}
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    isc.onServiceAdd,
		UpdateFunc: isc.onServiceUpdate,
	})

	isc.serviceLister = serviceInformer.Lister()

	return isc
}

func (isc *idleStatusController) Run(stopCh <-chan struct{}) {

	klog.Infof("Starting idling GracePeriod queue worker")

	wait.Until(isc.worker, time.Second, stopCh)

	klog.Infof("Shut idling GracePeriod queue worker")
	isc.gracePeriodQueue.ShutDown()
}

func (isc *idleStatusController) worker() {
	for isc.processNextWorkItem() {
	}
}

func (isc *idleStatusController) processNextWorkItem() bool {
	key, shutdown := isc.gracePeriodQueue.Get()

	// if we have to shutdown, return now
	if shutdown {
		return false
	}

	err := func(key interface{}) error {
		namespace, name, err := cache.SplitMetaNamespaceKey(key.(string))
		if err != nil {
			return err
		}
		klog.Infof("Grace period finished for service %s/%s", namespace, name)

		// Get current Service from the cache
		service, err := isc.serviceLister.Services(namespace).Get(name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Service has been deleted during the grace period
				return nil
			}

			return err
		}

		err = isc.markServiceAsNotIdle(service)
		if err != nil {
			return err
		}

		return nil
	}(key)

	if err != nil {
		utilruntime.HandleError(err)
	}

	return true
}

func (isc *idleStatusController) onServiceAdd(obj interface{}) {
	svc := obj.(*kapi.Service)

	isIdled := hasIdleAt(svc)
	if !isIdled {
		return
	}

	// Manage a Service that has been created in idle state
	err := isc.markServiceAsIdleIfNeeded(svc)
	if err != nil {
		utilruntime.HandleError(err)
	}
}

func (isc *idleStatusController) onServiceUpdate(old, new interface{}) {
	oldSvc := old.(*kapi.Service)
	newSvc := new.(*kapi.Service)

	oldSvcIdleAt := hasIdleAt(oldSvc)
	newSvcIdleAt := hasIdleAt(newSvc)

	if oldSvcIdleAt && !newSvcIdleAt {
		// Service has been unidled, put it in the grace period
		err := isc.markServiceAsGracePeriod(newSvc)
		if err != nil {
			utilruntime.HandleError(err)
		}

		return
	}

	if newSvcIdleAt {
		err := isc.markServiceAsIdleIfNeeded(newSvc)
		if err != nil {
			utilruntime.HandleError(err)
		}

		return
	}

}

func (isc *idleStatusController) onServiceDelete(obj interface{}) error {
	return nil
}

func (isc *idleStatusController) markServiceAsGracePeriod(svc *kapi.Service) error {
	// Service has been unidled, put it in the grace period
	err := isc.kube.SetAnnotationsOnService(svc.Namespace, svc.Name, map[string]interface{}{
		StatusAnnotation: StatusGracePeriod,
	})
	if err != nil {
		return fmt.Errorf("can't set service idle status to [%s]: %w", StatusGracePeriod, err)
	}

	key, err := cache.MetaNamespaceKeyFunc(svc)
	if err != nil {
		return fmt.Errorf("couldn't get key for service %+v: %v", svc, err)
	}

	isc.gracePeriodQueue.AddAfter(key, isc.gracePeriod)
	return nil
}

func (isc *idleStatusController) markServiceAsIdleIfNeeded(svc *kapi.Service) error {

	if svc.Annotations != nil {
		status, ok := svc.Annotations[StatusAnnotation]
		if ok && status == StatusIdle {
			// Service is already marked as Idle
			return nil
		}

	}

	err := isc.kube.SetAnnotationsOnService(svc.Namespace, svc.Name, map[string]interface{}{
		StatusAnnotation: StatusIdle,
	})
	if err != nil {
		return fmt.Errorf("can't set service idle status to [%s]: %w", StatusIdle, err)
	}

	return nil
}

func (isc *idleStatusController) markServiceAsNotIdle(svc *kapi.Service) error {

	if svc.Annotations == nil {
		return nil
	}

	status, ok := svc.Annotations[StatusAnnotation]
	if !ok || status == StatusNotIdle {
		// Service is already marked as NotIdle
		return nil
	}

	if status == StatusIdle {
		// Service has been idled again during the grace period
		return nil
	}

	err := isc.kube.SetAnnotationsOnService(svc.Namespace, svc.Name, map[string]interface{}{
		StatusAnnotation: StatusNotIdle,
	})
	if err != nil {
		return fmt.Errorf("can't set service idle status to [%s]: %w", StatusNotIdle, err)
	}

	return nil
}

func hasIdleAt(svc *kapi.Service) bool {
	if svc == nil {
		return false
	}

	if svc.Annotations == nil {
		return false
	}

	for annotationKey := range svc.Annotations {
		if strings.HasSuffix(annotationKey, OvnServiceIdledSuffix) {
			return true
		}
	}

	return false
}

func GetIdleStatus(svc *kapi.Service) Status {

	if svc.Annotations == nil {
		return StatusNotIdle
	}

	status, ok := svc.Annotations[StatusAnnotation]
	if !ok {
		return StatusNotIdle
	}

	return Status(status)

}
