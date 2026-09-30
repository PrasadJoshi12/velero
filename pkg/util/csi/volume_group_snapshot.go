/*
Copyright the Velero contributors.

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

package csi

import (
	"context"

	"github.com/cockroachdb/errors"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
)

const VGSBackupFinalizer = "velero.io/volume-group-snapshot-cleanup"

func EnsureVGSBackupFinalizer(ctx context.Context, backup *velerov1.Backup, c crclient.Client) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &velerov1.Backup{}
		if err := c.Get(ctx, crclient.ObjectKeyFromObject(backup), current); err != nil {
			return err
		}
		if current.UID != backup.UID || !current.DeletionTimestamp.IsZero() {
			return errors.New("backup changed or is being deleted")
		}
		if controllerutil.ContainsFinalizer(current, VGSBackupFinalizer) {
			return nil
		}
		base := current.DeepCopy()
		controllerutil.AddFinalizer(current, VGSBackupFinalizer)
		return c.Patch(ctx, current, crclient.MergeFrom(base))
	})
}

// CleanupBackupVolumeGroupSnapshots removes temporary group snapshot API objects
// after the finalized backup is persisted. Cleanup is idempotent and errors are
// returned so the terminal-backup reconciliation can retry.
func CleanupBackupVolumeGroupSnapshots(ctx context.Context, backup *velerov1.Backup, client crclient.Client, log logrus.FieldLogger) error {
	if backup.UID == "" || (backup.Status.Phase != velerov1.BackupPhaseCompleted && backup.Status.Phase != velerov1.BackupPhasePartiallyFailed && backup.Status.Phase != velerov1.BackupPhaseFailed) {
		return nil
	}

	groups, err := ListVGS(ctx, client, "", map[string]string{
		velerov1.BackupNameLabel: label.GetValidName(backup.Name),
		velerov1.BackupUIDLabel:  string(backup.UID),
	})
	if err != nil {
		if errors.Is(err, ErrVGSAPINotAvailable) {
			return nil
		}
		return errors.Wrap(err, "listing backup VolumeGroupSnapshots")
	}
	contents, err := ListVGSC(ctx, client, map[string]string{velerov1.BackupUIDLabel: string(backup.UID)})
	if err != nil && !errors.Is(err, ErrVGSAPINotAvailable) {
		return errors.Wrap(err, "listing backup VolumeGroupSnapshotContents")
	}
	if err == nil {
		for i := range contents.Items {
			content := &contents.Items[i]
			content.Spec.DeletionPolicy = snapshotv1.VolumeSnapshotContentRetain
			if _, err := UpdateVGSC(ctx, client, content); err != nil {
				return errors.Wrapf(err, "retaining VolumeGroupSnapshotContent %s", content.Name)
			}
			if err := DeleteVGSC(ctx, client, content.Name); err != nil && !apierrors.IsNotFound(err) {
				return errors.Wrapf(err, "deleting VolumeGroupSnapshotContent %s", content.Name)
			}
		}
	}

	for i := range groups.Items {
		group := &groups.Items[i]
		if group.Status != nil && group.Status.BoundVolumeGroupSnapshotContentName != nil {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				content, err := GetVGSC(ctx, client, *group.Status.BoundVolumeGroupSnapshotContentName)
				if err != nil {
					if apierrors.IsNotFound(err) {
						return nil
					}
					return err
				}
				ref := content.Spec.VolumeGroupSnapshotRef
				if ref.UID != group.UID || ref.Name != group.Name || ref.Namespace != group.Namespace {
					return errors.Errorf("VolumeGroupSnapshotContent %s is not bound to %s/%s", content.Name, group.Namespace, group.Name)
				}
				kubeutil.AddLabels(&content.ObjectMeta, map[string]string{
					velerov1.BackupNameLabel: label.GetValidName(backup.Name),
					velerov1.BackupUIDLabel:  string(backup.UID),
				})
				content.Spec.DeletionPolicy = snapshotv1.VolumeSnapshotContentRetain
				if _, err := UpdateVGSC(ctx, client, content); err != nil {
					return err
				}
				return DeleteVGSC(ctx, client, content.Name)
			})
			if err != nil {
				return errors.Wrapf(err, "cleaning up content of VolumeGroupSnapshot %s/%s", group.Namespace, group.Name)
			}
		}

		log.Infof("Cleaning up VolumeGroupSnapshot %s/%s after backup finalization", group.Namespace, group.Name)
		if err := DeleteVGS(ctx, client, group.Namespace, group.Name); err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "deleting VolumeGroupSnapshot %s/%s", group.Namespace, group.Name)
		}
	}
	remaining, err := ListVGS(ctx, client, "", map[string]string{velerov1.BackupUIDLabel: string(backup.UID)})
	if err != nil && !errors.Is(err, ErrVGSAPINotAvailable) {
		return errors.Wrap(err, "verifying VolumeGroupSnapshot cleanup")
	}
	if err == nil && len(remaining.Items) > 0 {
		return errors.New("VolumeGroupSnapshot cleanup is still pending")
	}
	remainingContents, err := ListVGSC(ctx, client, map[string]string{velerov1.BackupUIDLabel: string(backup.UID)})
	if err != nil && !errors.Is(err, ErrVGSAPINotAvailable) {
		return errors.Wrap(err, "verifying VolumeGroupSnapshotContent cleanup")
	}
	if err == nil && len(remainingContents.Items) > 0 {
		return errors.New("VolumeGroupSnapshotContent cleanup is still pending")
	}
	return nil
}
