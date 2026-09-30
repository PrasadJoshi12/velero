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

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
)

// CleanupBackupVolumeGroupSnapshots removes group snapshot API objects associated
// with a Backup. Cleanup is idempotent and errors are returned to the caller so
// normal Backup reconciliation or offline deletion cleanup can report them.
func CleanupBackupVolumeGroupSnapshots(ctx context.Context, backup *velerov1.Backup, client crclient.Client, log logrus.FieldLogger) error {
	if backup.UID == "" {
		return nil
	}

	groups, err := ListVGS(ctx, client, "", map[string]string{velerov1.BackupUIDLabel: string(backup.UID)})
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
	processedContents := map[string]struct{}{}
	if err == nil {
		for i := range contents.Items {
			content := &contents.Items[i]
			processedContents[content.Name] = struct{}{}
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
			contentName := *group.Status.BoundVolumeGroupSnapshotContentName
			_, processed := processedContents[contentName]
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				if processed {
					return nil
				}
				content, err := GetVGSC(ctx, client, contentName)
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
	return nil
}
