package storage

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	clientset "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type SnapshotStoreInterface interface {
	// Create a run entry in the database
	CreatePVC(pvcName string, size string, namespace string) error
	createSnapshot(pvcName string, snapshotName string, namespace string) error
}

type SnapshotStore struct {
	db        *DB
	csiClient clientset.Interface
	uuid      util.UUIDGeneratorInterface
	client    *kubernetes.Clientset
}

var SnapshotStoreColumn = []string{"UUID", "CreatedAt", "Class", "Name", "Namespace", "pvcName", "ContentName"}
var PvcColumn = []string{"UUID", "CreatedAt", "AccessMode", "Class", "Name", "Namespace"}

func (s *SnapshotStore) CreatePVC(pvcName string, size string, namespace string) (string, error) {
	// Create a PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvcName,
			Annotations: map[string]string{
				"kubeflow.org/pvc-name": pvcName,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}

	_, err := s.client.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			return "", fmt.Errorf("PVC %q already exists", pvcName)
		} else {
			return "", fmt.Errorf("Failed to create PVC %q: %v", pvcName, err)
		}
	}
	sql, args, err := sq.
		Insert("pvcs").
		SetMap(
			sq.Eq{
				"UUID":       pvc.UID,
				"CreatedAt":  pvc.CreationTimestamp,
				"AccessMode": pvc.Spec.AccessModes,
				"Class":      pvc.Spec.StorageClassName,
				"Name":       pvc.Name,
				"Namespace":  pvc.Namespace}).
		ToSql()
	if err != nil {
		return "", util.NewInternalServerError(err, "Failed to create query to insert pvc to pvc table: %v",
			err.Error())
	}
	tx, err := s.db.Begin()
	if err != nil {
		return "", util.NewInternalServerError(err,
			`Failed to start a transaction to create a new pvc: %v`,
			err.Error())
	}
	_, err = tx.Exec(sql, args...)
	if err != nil {
		tx.Rollback()
		return "", util.NewInternalServerError(err,
			"Failed to add pvc to pvc table: %v",
			err.Error())
	}
	if err := tx.Commit(); err != nil {
		return "", util.NewInternalServerError(err,
			`Failed to update pvc and pvc in a
			transaction: %v`, err.Error())
	}
	return pvcName, nil
}

func (s *SnapshotStore) createSnapshot(pvcName string, snapshotName string, namespace string) error {
	// Create a new VolumeSnapshot object
	SnapshotClass := "longhorn"

	snapshot := &snapshotv1.VolumeSnapshot{

		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
			VolumeSnapshotClassName: &SnapshotClass,
		},
	}
	// Use the CSI client to create the snapshot
	_, err := s.csiClient.SnapshotV1().VolumeSnapshots(snapshot.Namespace).Create(context.TODO(), snapshot, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	sql, args, err := sq.
		Insert("snapshots").
		SetMap(
			sq.Eq{
				"UUID":        snapshot.UID,
				"CreatedAt":   snapshot.CreationTimestamp,
				"Class":       snapshot.Spec.VolumeSnapshotClassName,
				"Name":        snapshot.Name,
				"Namespace":   snapshot.Namespace,
				"pvcName":     snapshot.Spec.Source.PersistentVolumeClaimName,
				"ContentName": snapshot.Spec.Source.VolumeSnapshotContentName}).
		ToSql()
	if err != nil {
		return util.NewInternalServerError(err, "Failed to create query to insert snapshot to snapshot table: %v",
			err.Error())
	}
	tx, err := s.db.Begin()
	if err != nil {
		return util.NewInternalServerError(err,
			`Failed to start a transaction to create a new snapshot: %v`,
			err.Error())
	}
	_, err = tx.Exec(sql, args...)
	if err != nil {
		tx.Rollback()
		return util.NewInternalServerError(err,
			"Failed to add snapshot to snapshot table: %v",
			err.Error())
	}
	if err := tx.Commit(); err != nil {
		return util.NewInternalServerError(err,
			`Failed to update snapshot and snapshot in a
			transaction: %v`, err.Error())
	}
	return nil
}
