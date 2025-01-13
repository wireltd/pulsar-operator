/*
 * Copyright 2021 - now, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pulsarcluster

import (
	"context"
	"fmt"
	"github.com/monimesl/operator-helper/k8s"
	"github.com/monimesl/operator-helper/k8s/job"
	"github.com/monimesl/operator-helper/reconciler"
	"github.com/monimesl/pulsar-operator/api/v1alpha1"
	v1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
)

func ReconcileJob(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) error {
	return reconcileClusterMetadataInitJob(ctx, cluster)
}

func reconcileClusterMetadataInitJob(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) error {
	if cluster.Status.Metadata.Stage == "" { // the cluster is just created
		jb := &v1.Job{}
		return ctx.GetResource(types.NamespacedName{
			Namespace: cluster.Namespace,
			Name:      initializeClusterMetadata(cluster),
		}, jb,
			func() (err error) { // Job already exists
				if jb.Status.Succeeded > 0 {
					// Update the PulsarCluster Status Stage to Launching
					cluster.Status.Metadata.Stage = v1alpha1.ClusterStageInitialized
					if err = ctx.Client().Status().Update(context.TODO(), cluster); err == nil {
						ctx.Logger().Info("Pulsar cluster metadata initialization successful. ",
							"cluster", cluster.GetName(),
							"Job.Name", jb.GetName(),
							"Job.Namespace", jb.GetNamespace())
					}
				} else if jb.Status.Failed > 0 {
					err1 := fmt.Errorf("pulsar cluster metadata "+
						"initialization error: %s", jb.GetName())
					ctx.Logger().Error(err,
						err1.Error(),

						"cluster", cluster.GetName(),
						"Job.Name", jb.GetName(),
						"Job.Namespace", jb.GetNamespace(),
						"Job.FailureCount", jb.Status.Failed)
					err = err1
				}
				return err
			},
			func() (err error) { // Job does not exists
				jb = createClusterMetadataInitJob(cluster)
				if err = ctx.SetOwnershipReference(cluster, jb); err == nil {
					if err = ctx.Client().Create(context.TODO(), jb); err == nil {
						ctx.Logger().Info("Pulsar cluster metadata init job created successfully ",
							"Job.Name", jb.GetName(),
							"Job.Namespace", jb.GetNamespace())
					}
				}
				return err
			})
	}
	return nil
}

func createClusterMetadataInitJob(c *v1alpha1.PulsarCluster) *v1.Job {
	labels := c.GenerateLabels(false)
	return job.New(jobNamespace(c), initializeClusterMetadata(c), labels,
		v1.JobSpec{
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: initializeClusterMetadata(c),
					Namespace:    c.Namespace,
					Labels:       labels,
				},
				Spec: coreV1.PodSpec{
					RestartPolicy: coreV1.RestartPolicyOnFailure,
					Containers:    createJobPodSpecContainers(c),
				},
			},
		})
}

func createJobPodSpecContainers(c *v1alpha1.PulsarCluster) []coreV1.Container {
	initJobResources := c.Spec.InitJobResources
	if initJobResources.Limits == nil || initJobResources.Requests == nil {
		initJobResources = c.Spec.PodConfig.Spec.Resources
	}
	return []coreV1.Container{
		{
			Name:            "cluster-metadata-init",
			Image:           c.Image().ToString(),
			ImagePullPolicy: c.Image().PullPolicy,
			Resources:       initJobResources,
			Command:         k8s.ContainerShellCommand(),
			Args:            createJobPodContainerArguments(c),
			EnvFrom: []coreV1.EnvFromSource{
				{
					ConfigMapRef: &coreV1.ConfigMapEnvSource{
						LocalObjectReference: coreV1.LocalObjectReference{
							Name: c.ConfigMapName(),
						},
					},
				},
			},
		},
	}
}

func createJobPodContainerArguments(c *v1alpha1.PulsarCluster) []string {
	serviceUrl := c.ClientHeadlessServiceFQDN()
	args := []string{
		"bin/pulsar initialize-cluster-metadata",
		fmt.Sprintf("--cluster %s", c.GetName()),
		fmt.Sprintf("--zookeeper %s ", c.Spec.ZookeeperServers),
		fmt.Sprintf("--configuration-store %s", c.Spec.ConfigurationStoreServers),
		fmt.Sprintf("--web-service-url %s:%d", serviceUrl, c.Spec.Ports.Web),
		fmt.Sprintf("--web-service-url-tls %s:%d", serviceUrl, c.Spec.Ports.WebTLS),
		fmt.Sprintf("--broker-service-url %s:%d", serviceUrl, c.Spec.Ports.Client),
		fmt.Sprintf("--broker-service-url-tls %s:%d", serviceUrl, c.Spec.Ports.ClientTLS),
	}
	if c.Spec.BookkeeperClusterUri != "" && c.Spec.VersionInt() >= 270 {
		args = append(args,
			fmt.Sprintf("--existing-bk-metadata-service-uri \"%s\"", c.Spec.BookkeeperClusterUri),
		)
	} else if c.Spec.BookkeeperClusterUri != "" && c.Spec.VersionInt() >= 262 {
		args = append(args,
			//  For compatibility of the command, we're passing the old flag to mean the same thing
			fmt.Sprintf("--bookkeeper-metadata-service-uri \"%s\"", c.Spec.BookkeeperClusterUri),
		)
	}
	args = append(args,
		// In case we have istio sidecar injected into the Job
		" && curl -sf -X POST http://127.0.0.1:15020/quitquitquit",
	)
	return []string{strings.Join(args, " ")}
}

func initializeClusterMetadata(c *v1alpha1.PulsarCluster) string {
	return fmt.Sprintf("%s-cluster-metadata-init-job", c.GetName())
}

func jobNamespace(c *v1alpha1.PulsarCluster) string {
	return c.GetNamespace()
}
