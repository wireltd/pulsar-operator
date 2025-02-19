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

//nolint:dupl
package controller

import (
	"context"
	"github.com/monimesl/operator-helper/reconciler"
	v12 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	v13 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pulsarv1alpha1 "github.com/monimesl/pulsar-operator/api/v1alpha1"
)

var (
	_                     reconciler.Context    = &PulsarManagerReconciler{}
	_                     reconciler.Reconciler = &PulsarManagerReconciler{}
	managerReconcileFuncs                       = []func(ctx reconciler.Context, cluster *pulsarv1alpha1.PulsarManager) error{}
)

// PulsarManagerReconciler reconciles a PulsarManager object
type PulsarManagerReconciler struct {
	reconciler.Context
}

// Configure configures the above PulsarManagerReconciler
func (r *PulsarManagerReconciler) Configure(ctx reconciler.Context) error {
	r.Context = ctx
	return ctx.NewControllerBuilder().
		For(&pulsarv1alpha1.PulsarManager{}).
		Owns(&v13.PodDisruptionBudget{}).
		Owns(&v12.StatefulSet{}).
		Owns(&v1.ConfigMap{}).
		Owns(&v1.Service{}).
		Complete(r)
}

// Reconcile handles reconciliation request for PulsarManager instances
func (r *PulsarManagerReconciler) Reconcile(_ context.Context, request reconcile.Request) (reconcile.Result, error) {
	cluster := &pulsarv1alpha1.PulsarManager{}
	return r.Run(request, cluster, func(_ bool) (err error) {
		for _, fun := range managerReconcileFuncs {
			if err = fun(r, cluster); err != nil {
				break
			}
		}
		return
	})
}
