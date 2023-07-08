/*
Copyright 2017 The Kubernetes Authors.

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

package controller

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
)

const (
	annMigratedTo = "pv.kubernetes.io/migrated-to"
)

// CSIAttachController is a controller that attaches / detaches CSI volumes using provided Handler interface
type CSIAttachController struct {
	client kubernetes.Interface
	// 1、这里的AttacherName实际上就是CSI插件的名字，这个名字通过GRPC调用CSI插件的Identity服务的GetPluginInfo接口获取
	// 2、每一种external-attacher都是伴随着一种CSI插件部署在一个Pod当中的，而这个external-attacher就是为了处理这个当前和它部署在
	// 一起的CSI插件相关的VolumeAttachment资源对象，因此external-attacher就是跟着CSI插件姓的。似乎有点嫁鸡随鸡、家狗随狗的意味！！！
	attacherName  string
	handler       Handler
	eventRecorder record.EventRecorder
	vaQueue       workqueue.RateLimitingInterface
	pvQueue       workqueue.RateLimitingInterface

	vaLister       storagelisters.VolumeAttachmentLister
	vaListerSynced cache.InformerSynced
	pvLister       corelisters.PersistentVolumeLister
	pvListerSynced cache.InformerSynced

	// 实际上，这个值是通过supportsListVolumesPublishedNodes变量传进来的，也就是说如果CSI创建支持列出某个节点上已经创建并且Attach
	// 的卷，那么external-attacher就需要对于VolumeAttachment做处理
	shouldReconcileVolumeAttachment bool
	reconcileSync                   time.Duration
	translator                      AttacherCSITranslator

	defaultFSType string
}

// Handler is responsible for handling VolumeAttachment events from informer.
type Handler interface {
	// Init TODO 这里初始化了个啥？
	Init(vaQueue workqueue.RateLimitingInterface, pvQueue workqueue.RateLimitingInterface)

	// SyncNewOrUpdatedVolumeAttachment processes one Add/Updated event from
	// VolumeAttachment informers. It runs in a workqueue, guaranting that only
	// one SyncNewOrUpdatedVolumeAttachment runs for given VA.
	// SyncNewOrUpdatedVolumeAttachment is responsible for marking the
	// VolumeAttachment either as forgotten (resets exponential backoff) or
	// re-queue it into the vaQueue to process it after exponential
	// backoff.
	SyncNewOrUpdatedVolumeAttachment(va *storage.VolumeAttachment)

	SyncNewOrUpdatedPersistentVolume(pv *v1.PersistentVolume)

	ReconcileVA() error
}

// NewCSIAttachController returns a new *CSIAttachController
func NewCSIAttachController(client kubernetes.Interface, attacherName string, handler Handler,
	volumeAttachmentInformer storageinformers.VolumeAttachmentInformer, pvInformer coreinformers.PersistentVolumeInformer,
	vaRateLimiter, paRateLimiter workqueue.RateLimiter, shouldReconcileVolumeAttachment bool, reconcileSync time.Duration) *CSIAttachController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("csi-attacher %s", attacherName)})

	ctrl := &CSIAttachController{
		client:                          client,
		attacherName:                    attacherName,
		handler:                         handler,
		eventRecorder:                   eventRecorder,
		vaQueue:                         workqueue.NewNamedRateLimitingQueue(vaRateLimiter, "csi-attacher-va"),
		pvQueue:                         workqueue.NewNamedRateLimitingQueue(paRateLimiter, "csi-attacher-pv"),
		shouldReconcileVolumeAttachment: shouldReconcileVolumeAttachment,
		reconcileSync:                   reconcileSync,
		translator:                      csitrans.New(),
	}

	volumeAttachmentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.vaAdded,   // 监听VolumeAttachment的创建
		UpdateFunc: ctrl.vaUpdated, // 监听VolumeAttachment的更新
		DeleteFunc: ctrl.vaDeleted, // 监听VolumeAttachment的删除
	})
	ctrl.vaLister = volumeAttachmentInformer.Lister()
	ctrl.vaListerSynced = volumeAttachmentInformer.Informer().HasSynced

	pvInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.pvAdded,   // 监听PV的创建
		UpdateFunc: ctrl.pvUpdated, // 监听PV的更新
		//DeleteFunc: ctrl.pvDeleted, TODO: do we need this?
	})
	ctrl.pvLister = pvInformer.Lister()
	// 用于判断PV是否已经同步完成
	ctrl.pvListerSynced = pvInformer.Informer().HasSynced
	// 给Handler初始化VAQueue以及PVQueue
	ctrl.handler.Init(ctrl.vaQueue, ctrl.pvQueue)

	return ctrl
}

// Run starts CSI attacher and listens on channel events
func (ctrl *CSIAttachController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.vaQueue.ShutDown()
	defer ctrl.pvQueue.ShutDown()

	klog.Infof("Starting CSI attacher")
	defer klog.Infof("Shutting CSI attacher")

	// 等待VolumeAttachment, PV资源同步完成
	if !cache.WaitForCacheSync(stopCh, ctrl.vaListerSynced, ctrl.pvListerSynced) {
		klog.Errorf("Cannot sync caches")
		return
	}
	for i := 0; i < workers; i++ {
		// TODO 从VAQueue中取出元素并处理VolumeAttachment资源对象
		go wait.Until(ctrl.syncVA, 0, stopCh)
		// TODO 从PVQueue中取出元素并处理PersistentVolume资源对象
		go wait.Until(ctrl.syncPV, 0, stopCh)
	}

	if ctrl.shouldReconcileVolumeAttachment {
		go wait.Until(func() {
			// TODO 这里是在处理什么？
			err := ctrl.handler.ReconcileVA()
			if err != nil {
				klog.Errorf("Failed to reconcile volume attachments: %v", err)
			}
		}, ctrl.reconcileSync, stopCh)
	}

	<-stopCh
}

// vaAdded reacts to a VolumeAttachment creation
func (ctrl *CSIAttachController) vaAdded(obj interface{}) {
	va := obj.(*storage.VolumeAttachment)
	ctrl.vaQueue.Add(va.Name)
}

// vaUpdated reacts to a VolumeAttachment update
func (ctrl *CSIAttachController) vaUpdated(old, new interface{}) {
	oldVA := old.(*storage.VolumeAttachment)
	newVA := new.(*storage.VolumeAttachment)
	if shouldEnqueueVAChange(oldVA, newVA) {
		ctrl.vaQueue.Add(newVA.Name)
	} else {
		klog.V(3).Infof("Ignoring VolumeAttachment %q change", newVA.Name)
	}
}

// vaDeleted reacts to a VolumeAttachment deleted
func (ctrl *CSIAttachController) vaDeleted(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	va := obj.(*storage.VolumeAttachment)
	if va != nil && va.Spec.Source.PersistentVolumeName != nil {
		// Enqueue PV sync event - it will evaluate and remove finalizer
		ctrl.pvQueue.Add(*va.Spec.Source.PersistentVolumeName)
	}
}

// pvAdded reacts to a PV creation
func (ctrl *CSIAttachController) pvAdded(obj interface{}) {
	pv := obj.(*v1.PersistentVolume)
	if !ctrl.processFinalizers(pv) {
		return
	}
	ctrl.pvQueue.Add(pv.Name)
}

// pvUpdated reacts to a PV update
func (ctrl *CSIAttachController) pvUpdated(old, new interface{}) {
	pv := new.(*v1.PersistentVolume)
	if !ctrl.processFinalizers(pv) {
		return
	}
	ctrl.pvQueue.Add(pv.Name)
}

// syncVA deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSIAttachController) syncVA() {
	// 从VA对列中取出一个VolumeAttachment资源对象
	key, quit := ctrl.vaQueue.Get()
	if quit {
		return
	}
	defer ctrl.vaQueue.Done(key)

	vaName := key.(string)
	klog.V(4).Infof("Started VA processing %q", vaName)

	// 通过VA的名字在Informer中查找VolumeAttachment资源对象
	va, err := ctrl.vaLister.Get(vaName)
	if err != nil {
		// 如果没有找到，那么直接从VA队列中删除这个元素。
		if apierrs.IsNotFound(err) {
			// VolumeAttachment was deleted in the meantime, ignore.
			klog.V(3).Infof("VA %q deleted, ignoring", vaName)
			return
		}
		klog.Errorf("Error getting VolumeAttachment %q: %v", vaName, err)
		// 否则如果是其它错误（可能是网络错误），就把这个资源对象重新加入到VA队列当中
		ctrl.vaQueue.AddRateLimited(vaName)
		return
	}

	// 每隔被部署的external-attacher只能处理和自己部署在同一个Pod当中的CSI插件关心的VolumeAttacher。对于自己不关心的VolumeAttachment
	// 资源对象，一概忽视
	if va.Spec.Attacher != ctrl.attacherName {
		klog.V(4).Infof("Skipping VolumeAttachment %s for attacher %s", va.Name, va.Spec.Attacher)
		return
	}
	// TODO 处理VolumeAttachment
	ctrl.handler.SyncNewOrUpdatedVolumeAttachment(va)
}

func (ctrl *CSIAttachController) processFinalizers(pv *v1.PersistentVolume) bool {
	if sets.NewString(pv.Finalizers...).Has(GetFinalizerName(ctrl.attacherName)) {
		if pv.DeletionTimestamp != nil {
			return true
		}

		// if PV is provisioned by in-tree plugin and does not have migrated-to label
		// this normally means this is a rollback scenario, we need to remove the finalizer as well
		if ctrl.translator.IsPVMigratable(pv) {
			if ann := pv.Annotations; ann != nil {
				if migratedToDriver := ann[annMigratedTo]; migratedToDriver == ctrl.attacherName {
					// migrated-to annonation detected, keep the finalizer
					return false
				}
			}
			return true
		}
	}
	return false
}

// syncPV deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSIAttachController) syncPV() {
	key, quit := ctrl.pvQueue.Get()
	if quit {
		return
	}
	defer ctrl.pvQueue.Done(key)

	pvName := key.(string)
	klog.V(4).Infof("Started PV processing %q", pvName)

	// 从Informer中获取PV
	pv, err := ctrl.pvLister.Get(pvName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// PV was deleted in the meantime, ignore.
			klog.V(3).Infof("PV %q deleted, ignoring", pvName)
			return
		}
		klog.Errorf("Error getting PersistentVolume %q: %v", pvName, err)
		ctrl.pvQueue.AddRateLimited(pvName)
		return
	}
	ctrl.handler.SyncNewOrUpdatedPersistentVolume(pv)
}

// shouldEnqueueVAChange checks if a changed VolumeAttachment should be enqueued.
// It filters out changes in Status.Attach/DetachError - these were posted by the controller
// just few moments ago. If they were enqueued, Attach()/Detach() would be called again,
// breaking exponential backoff.
func shouldEnqueueVAChange(old, new *storage.VolumeAttachment) bool {
	if old.ResourceVersion == new.ResourceVersion {
		// This is most probably periodic sync, enqueue it
		return true
	}
	if new.Status.AttachError == nil && new.Status.DetachError == nil && old.Status.AttachError == nil && old.Status.DetachError == nil {
		// The difference between old and new must be elsewhere than Status.Attach/DetachError
		return true
	}

	sanitized := new.DeepCopy()
	sanitized.ResourceVersion = old.ResourceVersion
	sanitized.Status.AttachError = old.Status.AttachError
	sanitized.Status.DetachError = old.Status.DetachError
	sanitized.ManagedFields = old.ManagedFields

	if equality.Semantic.DeepEqual(old, sanitized) {
		// The objects are the same except Status.Attach/DetachError.
		// Don't enqueue them.
		return false
	}
	return true
}
