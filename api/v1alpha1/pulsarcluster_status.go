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

package v1alpha1

// ClusterStage represents the stage of the pulsar broker cluster
type ClusterStage string

const (
	// ClusterStageInitialized - cluster object is created but statefulset not created
	ClusterStageInitialized = "Initialized"
	// ClusterStageLaunched - cluster is initialized and the pods have been created but not ready
	ClusterStageLaunched = "Launched"
	// ClusterStageRunning - cluster is launched and running
	ClusterStageRunning = "Running"
)

// PulsarClusterStatus defines the observed state of PulsarCluster
type PulsarClusterStatus struct {

	// Metadata defines the metadata status of the cluster
	// +optional
	Metadata Metadata `json:"metadata,omitempty"`
}

// Metadata defines the metadata status of the cluster
type Metadata struct {
	Size          int32             `json:"size,omitempty"`
	Stage         ClusterStage      `json:"stage,omitempty"`
	PulsarVersion string            `json:"pulsarVersion,omitempty"`
	BrokerConfig  map[string]string `json:"brokerConfig,omitempty"`
}

func (in *PulsarClusterStatus) setDefaults() (changed bool) {
	return
}
