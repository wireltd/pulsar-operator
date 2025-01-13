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
	"github.com/monimesl/operator-helper/k8s"
	"github.com/monimesl/operator-helper/reconciler"
	"github.com/monimesl/pulsar-operator/api/v1alpha1"
	v1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"math"
)

// ReconcilePodDisruptionBudget reconcile the poddisruptionbudget of the specified cluster
func ReconcilePodDisruptionBudget(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) error {
	return reconcilePodDisruptionBudget(ctx, cluster)
}

func reconcilePodDisruptionBudget(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) (err error) {
	pdb := &v1.PodDisruptionBudget{}
	return ctx.GetResource(types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, pdb,
		// Found
		func() error {
			if shouldUpdatePDB(cluster.Spec, pdb) {
				if err = updatePodDisruptionBudget(ctx, pdb, cluster); err != nil {
					return err
				}
				return nil
			}
			return nil
		},
		// Not Found
		func() error {
			pdb = createPodDisruptionBudget(cluster)
			if err := ctx.SetOwnershipReference(cluster, pdb); err != nil {
				return err
			}
			ctx.Logger().Info("Creating the zookeeper poddisruptionbudget for cluster",
				"cluster", cluster.Name,
				"PodDisruptionBudget.Name", pdb.GetName(),
				"PodDisruptionBudget.Namespace", pdb.GetNamespace(),
				"MaxUnavailable", pdb.Spec.MaxUnavailable.IntVal)
			return ctx.Client().Create(context.TODO(), pdb)
		},
	)
}

func calculateMaxAllowedFailureNodes(cluster *v1alpha1.PulsarCluster) intstr.IntOrString {
	if *cluster.Spec.Size < 3 {
		// For less than 3 nodes, we tolerate no node failure
		return intstr.FromInt32(0)
	}
	// In zookeeper, if you can tolerate a node failure count of `F`
	// then you need `2F+1` nodes to form a quorum of healthy nodes.
	// i.f N = 2F + 1 => F = (N-1) / 2. Practically F = floor((N-1) / 2)
	i := int32(math.Floor(float64(*cluster.Spec.Size-1) / 2.0))
	return intstr.FromInt32(i)
}

func createPodDisruptionBudget(cluster *v1alpha1.PulsarCluster) *v1.PodDisruptionBudget {
	newMaxFailureNodes := calculateMaxAllowedFailureNodes(cluster)
	return &v1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    cluster.GenerateLabels(true),
		},
		Spec: v1.PodDisruptionBudgetSpec{
			MaxUnavailable: &newMaxFailureNodes,
			Selector: &metav1.LabelSelector{
				MatchLabels: cluster.GenerateLabels(true),
			},
		},
	}
}

func updatePodDisruptionBudget(ctx reconciler.Context, pdb *v1.PodDisruptionBudget, c *v1alpha1.PulsarCluster) error {
	newMaxFailureNodes := intstr.FromInt32(c.Spec.MaxUnavailableNodes)
	pdb.Labels = c.GenerateLabels(true)
	pdb.Spec.MaxUnavailable.IntVal = newMaxFailureNodes.IntVal
	pdb.Spec.Selector.MatchLabels = c.GenerateLabels(true)
	ctx.Logger().Info("Updating the bookkeeper poddisruptionbudget for cluster",
		"cluster", c.Name,
		"PodDisruptionBudget.Name", pdb.GetName(),
		"PodDisruptionBudget.Namespace", pdb.GetNamespace(),
		"MaxUnavailable", pdb.Spec.MaxUnavailable.IntVal)
	return ctx.Client().Update(context.TODO(), pdb)
}

func shouldUpdatePDB(spec v1alpha1.PulsarClusterSpec, pdb *v1.PodDisruptionBudget) bool {
	if spec.PulsarVersion != pdb.Labels[k8s.LabelAppVersion] {
		return true
	}
	newMaxFailureNodes := intstr.FromInt32(spec.MaxUnavailableNodes)
	return newMaxFailureNodes.IntVal != pdb.Spec.MaxUnavailable.IntVal
}
