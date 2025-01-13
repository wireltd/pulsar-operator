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
	"github.com/monimesl/operator-helper/k8s/configmap"
	"github.com/monimesl/operator-helper/reconciler"
	"github.com/monimesl/pulsar-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
)

// ReconcileConfigMap reconcile the configmap of the specified cluster
func ReconcileConfigMap(ctx reconciler.Context, cluster *v1alpha1.PulsarCluster) error {
	cm := &v1.ConfigMap{}
	return ctx.GetResource(types.NamespacedName{
		Name:      cluster.ConfigMapName(),
		Namespace: cluster.Namespace,
	}, cm,
		// Found
		func() error {
			if shouldUpdateConfigmap(ctx, cluster) {
				if err := updateConfigmap(ctx, cm, cluster); err != nil {
					return err
				}
			}
			return nil
		},
		// Not Found
		func() (err error) {
			cm = createConfigMap(cluster)
			if err = ctx.SetOwnershipReference(cluster, cm); err == nil {
				ctx.Logger().Info("Creating the pulsar configMap",
					"ConfigMap.Name", cm.GetName(),
					"ConfigMap.Namespace", cm.GetNamespace())
				if err = ctx.Client().Create(context.TODO(), cm); err == nil {
					ctx.Logger().Info("ConfigMap creation success.",
						"ConfigMap.Name", cm.GetName(),
						"ConfigMap.Namespace", cm.GetNamespace())
				}
			}
			return
		})
}

func shouldUpdateConfigmap(ctx reconciler.Context, c *v1alpha1.PulsarCluster) bool {
	if !mapEqual(c.Spec.BrokerConfig, c.Status.Metadata.BrokerConfig) {
		ctx.Logger().Info("Broker cluster config changed",
			"from", c.Status.Metadata.BrokerConfig, "to", c.Spec.BrokerConfig,
		)
		return true
	}
	return false
}

func createConfigMap(c *v1alpha1.PulsarCluster) *v1.ConfigMap {
	data := createConfigmapData(c)
	cm := configmap.New(c.Namespace, c.ConfigMapName(), data)
	cm.Labels = c.GenerateLabels(false)
	return cm
}

func updateConfigmap(ctx reconciler.Context, cm *v1.ConfigMap, c *v1alpha1.PulsarCluster) error {
	ctx.Logger().Info("Updating the bookkeeper configmap.",
		"configMap.Name", cm.GetName(),
		"ConfigMap.Namespace", cm.GetNamespace(), "NewReplicas", c.Spec.Size)
	cm.Labels = c.GenerateLabels(false)
	cm.Data = createConfigmapData(c)
	return ctx.Client().Update(context.TODO(), cm)
}

func createConfigmapData(c *v1alpha1.PulsarCluster) map[string]string {
	jvmOptions := c.Spec.JVMOptions
	data := processEnvVarMap(map[string]string{
		"managedLedgerDefaultEnsembleSize": "2",
		"managedLedgerDefaultWriteQuorum":  "2",
		"managedLedgerDefaultAckQuorum":    "2",
		"statusFilePath":                   "/pulsar/status",
		"clusterName":                      c.GetName(),
		"zookeeperServers":                 c.Spec.ZookeeperServers,
		"bookkeeperMetadataServiceUri":     c.Spec.BookkeeperClusterUri,
		"configurationStoreServers":        c.Spec.ConfigurationStoreServers,
		"PULSAR_GC":                        strings.Join(jvmOptions.Gc, " "),
		"PULSAR_EXTRA_OPTS":                strings.Join(jvmOptions.Extra, " "),
		"PULSAR_MEM":                       strings.Join(jvmOptions.Memory, " "),
		"PULSAR_GC_LOG":                    strings.Join(jvmOptions.GcLogging, " "),
	}, false)
	for k, v := range processEnvVarMap(c.Spec.BrokerConfig, true) {
		data[k] = v
	}
	return processEnvVarMap(data, false)
}
