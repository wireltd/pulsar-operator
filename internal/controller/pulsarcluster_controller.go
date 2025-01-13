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

package controller

import (
	"context"
	"github.com/monimesl/operator-helper/reconciler"
	pulsarcluster2 "github.com/monimesl/pulsar-operator/internal/controller/pulsarcluster"
	v12 "k8s.io/api/apps/v1"
	v13 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	v14 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pulsarv1alpha1 "github.com/monimesl/pulsar-operator/api/v1alpha1"
)

var (
	_                     reconciler.Context    = &PulsarClusterReconciler{}
	_                     reconciler.Reconciler = &PulsarClusterReconciler{}
	clusterReconcileFuncs                       = []func(ctx reconciler.Context, cluster *pulsarv1alpha1.PulsarCluster) error{
		pulsarcluster2.ReconcilePodDisruptionBudget,
		pulsarcluster2.ReconcileServices,
		pulsarcluster2.ReconcileConfigMap,
		pulsarcluster2.ReconcileJob,
		pulsarcluster2.ReconcileStatefulSet,
		pulsarcluster2.ReconcileClusterStatus,
	}
)

// PulsarClusterReconciler reconciles a PulsarCluster object
type PulsarClusterReconciler struct {
	reconciler.Context
}

// Configure configures the above PulsarClusterReconciler
func (r *PulsarClusterReconciler) Configure(ctx reconciler.Context) error {
	r.Context = ctx
	return ctx.NewControllerBuilder().
		For(&pulsarv1alpha1.PulsarCluster{}).
		Owns(&v14.PodDisruptionBudget{}).
		Owns(&v12.StatefulSet{}).
		Owns(&v1.ConfigMap{}).
		Owns(&v1.Service{}).
		Owns(&v13.Job{}).
		Complete(r)
}

// Reconcile handles reconciliation request for PulsarCluster instances
func (r *PulsarClusterReconciler) Reconcile(_ context.Context, request reconcile.Request) (reconcile.Result, error) {
	cluster := &pulsarv1alpha1.PulsarCluster{}
	return r.Run(request, cluster, func(_ bool) (err error) {
		for _, fun := range clusterReconcileFuncs {
			if err = fun(r, cluster); err != nil {
				break
			}
		}
		return
	})
}
