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
	"github.com/monimesl/operator-helper/k8s/service"
	"github.com/monimesl/operator-helper/reconciler"
	"github.com/monimesl/pulsar-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileServices reconcile the services of the specified cluster
func ReconcileServices(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) (err error) {
	if err = reconcileHeadlessService(ctx, cluster); err == nil {
		err = reconcileClientService(ctx, cluster)
	}
	return
}

func reconcileClientService(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) error {
	svc := &v1.Service{}
	return ctx.GetResource(types.NamespacedName{
		Name:      cluster.ClientServiceName(),
		Namespace: cluster.Namespace,
	}, svc,
		// Found
		nil,
		// Not Found
		func() (err error) {
			svc = createClientService(cluster)
			if err = ctx.SetOwnershipReference(cluster, svc); err == nil {
				ctx.Logger().Info("Creating the pulsar client service.",
					"Service.Name", svc.GetName(),
					"Service.Namespace", svc.GetNamespace())
				if err = ctx.Client().Create(context.TODO(), svc); err == nil {
					ctx.Logger().Info("Service creation success.",
						"Service.Name", svc.GetName(),
						"Service.Namespace", svc.GetNamespace())
				}
			}
			return
		})
}

func reconcileHeadlessService(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) error {
	svc := &v1.Service{}
	return ctx.GetResource(types.NamespacedName{
		Name:      cluster.HeadlessServiceName(),
		Namespace: cluster.Namespace,
	}, svc,
		// Found
		nil,
		// Not Found
		func() (err error) {
			svc = createHeadlessService(cluster)
			if err = ctx.SetOwnershipReference(cluster, svc); err == nil {
				ctx.Logger().Info("Creating the pulsar headless service.",
					"Service.Name", svc.GetName(),
					"Service.Namespace", svc.GetNamespace())
				if err = ctx.Client().Create(context.TODO(), svc); err == nil {
					ctx.Logger().Info("Service creation success.",
						"Service.Name", svc.GetName(),
						"Service.Namespace", svc.GetNamespace())
				}
			}
			return
		})
}

func createClientService(c *v1alpha1.PulsarCluster) *v1.Service {
	return createService(c, c.ClientServiceName(), true, servicePorts(c))
}

func createHeadlessService(c *v1alpha1.PulsarCluster) *v1.Service {
	return createService(c, c.HeadlessServiceName(), false, servicePorts(c))
}

func createService(c *v1alpha1.PulsarCluster, name string, hasClusterIP bool, servicePorts []v1.ServicePort) *v1.Service {
	labels := c.GenerateLabels(true)
	clusterIP := ""
	if !hasClusterIP {
		clusterIP = v1.ClusterIPNone
	}
	srv := service.New(c.Namespace, name, labels, v1.ServiceSpec{
		ClusterIP: clusterIP,
		Selector:  labels,
		Ports:     servicePorts,
	})
	srv.Annotations = c.GenerateAnnotations()
	return srv
}

//nolint:dupl
func servicePorts(c *v1alpha1.PulsarCluster) []v1.ServicePort {
	ports := c.Spec.Ports
	svcPorts := []v1.ServicePort{{Name: v1alpha1.ClientPortName, Port: ports.Client}}
	if ports.ClientTLS > 0 {
		svcPorts = append(svcPorts, v1.ServicePort{Name: v1alpha1.ClientTLSPortName, Port: ports.ClientTLS})
	}
	if ports.Web > 0 {
		svcPorts = append(svcPorts, v1.ServicePort{Name: v1alpha1.WebPortName, Port: ports.Web})
	}
	if ports.WebTLS > 0 {
		svcPorts = append(svcPorts, v1.ServicePort{Name: v1alpha1.WebTLSPortName, Port: ports.WebTLS})
	}
	kop := c.Spec.KOP
	if kop.Enabled {
		if kop.PlainTextPort > 0 {
			svcPorts = append(svcPorts, v1.ServicePort{Name: v1alpha1.KopPlainTextPortName, Port: kop.PlainTextPort})
		}
		if kop.SecuredPort > 0 {
			svcPorts = append(svcPorts, v1.ServicePort{Name: v1alpha1.KopSecuredPortName, Port: kop.SecuredPort})
		}
	}
	return svcPorts
}
