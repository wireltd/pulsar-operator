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

import (
	"github.com/monimesl/operator-helper/basetype"
	"github.com/monimesl/operator-helper/k8s"
	"github.com/monimesl/operator-helper/k8s/pod"
	"github.com/monimesl/pulsar-operator/internal"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"math"
	"strconv"
	"strings"
)

const (
	imageRepository                   = "apachepulsar/pulsar"
	BrokerSetupImageRepository        = "monime/pulsar-broker-setup"
	DefaultBrokerSetupImageVersion    = "latest"
	DefaultBrokerSetupImagePullPolicy = "Always"
	defaultImageTag                   = "3.0.8"
	defaultBrokerPersistenceSize      = "8Gi"
)

const (
	ClientPortName       = "tcp-client"
	ClientTLSPortName    = "tls-client"
	WebPortName          = "http-web"
	WebTLSPortName       = "https-web"
	KopPlainTextPortName = "tcp-kop"
	KopSecuredPortName   = "tls-kop"
)

const (
	minimumClusterSize   = 3
	defaultClusterDomain = "cluster.local"
)

const (
	defaultClientPort    = 6650
	defaultClientTLSPort = 6651
	defaultWebPort       = 8080
	defaultWebTLSPort    = 8443
	defaultKopPlainPort  = 9092
	defaultKopSSLPort    = 9093
)

var (
	defaultTerminationGracePeriod int64 = 30
	defaultClusterSize                  = int32(minimumClusterSize)
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PulsarClusterSpec defines the desired state of PulsarCluster
type PulsarClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// ZookeeperServers specifies the hostname/IP address and port in the format "hostname:port".
	// +kubebuilder:validation:Required
	ZookeeperServers string `json:"zookeeperServers"`
	// BookkeeperClusterUri specifies the URI of the existing BookKeeper cluster that you want to use.
	// see https://pulsar.apache.org/docs/en/reference-cli-tools/#initialize-cluster-metadata
	// +kubebuilder:validation:Required
	BookkeeperClusterUri string `json:"bookkeeperClusterUri"`
	// ConfigurationStoreServers specifies the configuration store connection string (as a comma-separated list)
	// +optional
	ConfigurationStoreServers string `json:"configurationStoreServers"`
	// PulsarVersion defines the version of broker to use
	// +optional
	PulsarVersion string `json:"pulsarVersion,omitempty"`
	// ImagePullPolicy describes a policy for if/when to pull the image
	// +optional
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Size *int32 `json:"size,omitempty"`
	// KOP configures the Kafka Protocol Handler
	KOP        KOP       `json:"kop,omitempty"`
	Connectors Connector `json:"connectors,omitempty"`
	// MaxUnavailableNodes defines the maximum number of nodes that
	// can be unavailable as per kubernetes PodDisruptionBudget
	// Default is 1.
	// +optional
	MaxUnavailableNodes int32  `json:"maxUnavailableNodes"`
	Ports               *Ports `json:"ports,omitempty"`
	// BrokerConfig defines the Bookkeeper configurations to override the broker.conf
	// +optional
	BrokerConfig map[string]string `json:"brokerConfig"`
	// JVMOptions defines the JVM options for pulsar broker; this is useful for performance tuning.
	// If unspecified, a reasonable defaults will be set
	// +optional
	JVMOptions       JVMOptions                    `json:"jvmOptions"`
	InitJobResources v1.ResourceRequirements       `json:"initJobResources,omitempty"`
	Persistence      *v1.PersistentVolumeClaimSpec `json:"persistence,omitempty"`
	// PodConfig defines common configuration for the broker pods
	// +optional
	PodConfig basetype.PodConfig `json:"podConfig,omitempty"`
	// ProbeConfig defines the probing settings for the broker containers
	// +optional
	ProbeConfig *pod.Probes `json:"probeConfig,omitempty"`

	// Labels defines the labels to attach to the broker deployment
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations defines the annotations to attach to the broker statefulset and services
	Annotations map[string]string `json:"annotations,omitempty"`

	// ClusterDomain defines the cluster domain for the cluster
	// It defaults to cluster.local
	ClusterDomain string `json:"clusterDomain,omitempty"`
}

type MonitoringConfig struct {
	// Enabled defines whether this monitoring is enabled or not.
	Enabled bool `json:"enabled,omitempty"`
}

type Ports struct {
	// +kubebuilder:validation:Minimum=1
	Client int32 `json:"client,omitempty"`
	// +kubebuilder:validation:Minimum=1
	ClientTLS int32 `json:"clientTLS,omitempty"`
	// +kubebuilder:validation:Minimum=1
	Web int32 `json:"web,omitempty"`
	// +kubebuilder:validation:Minimum=1
	WebTLS int32 `json:"WebTLS,omitempty"`
}

type KOP struct {
	// Enabled defines whether this KOP is enabled or not.
	Enabled bool `json:"enabled,omitempty"`
	// < 0 means disabled
	// +optional
	PlainTextPort int32 `json:"plainTextPort,omitempty"`
	// < 0 means disabled
	SecuredPort int32 `json:"SecuredPort,omitempty"`
}

type JVMOptions struct {
	// Memory defines memory options
	// +optional
	Memory []string `json:"memory"`
	// Gc defines garbage collection options
	// +optional
	Gc []string `json:"gc"`
	// GcLogging defines garbage collection logging options
	// +optional
	GcLogging []string `json:"gcLogging"`
	// Extra defines extra options
	// +optional
	Extra []string `json:"extra"`
}

type Connector struct {
	Builtin []string                `json:"builtin,omitempty"`
	Custom  []CustomConnectorSource `json:"custom,omitempty"`
}

type CustomConnectorSource struct {
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (in *Ports) setDefaults() (changed bool) {
	if in.Client == 0 {
		changed = true
		in.Client = defaultClientPort
	}
	if in.ClientTLS == 0 {
		changed = true
		in.ClientTLS = defaultClientTLSPort
	}
	if in.Web == 0 {
		changed = true
		in.Web = defaultWebPort
	}
	if in.WebTLS == 0 {
		changed = true
		in.WebTLS = defaultWebTLSPort
	}
	return
}

func (in *JVMOptions) setDefaults() (changed bool) {
	if in.Memory == nil {
		changed = true
		in.Memory = []string{
			"-Xms128m", "-Xmx256m", "-XX:MaxDirectMemorySize=256m",
		}
	}
	if in.Gc == nil {
		changed = true
		in.Gc = []string{}
	}
	if in.GcLogging == nil {
		changed = true
		in.GcLogging = []string{}
	}
	if in.Extra == nil {
		changed = true
		in.Extra = strings.Split(
			"-Dio.netty.leakDetectionLevel=disabled "+
				"-Dio.netty.recycler.maxCapacity.default=1000 "+
				"-Dio.netty.recycler.linkCapacity=1024", " ")
	}
	return changed
}

// setDefaults set the defaults for the cluster spec and returns true otherwise false
//
//nolint:cyclop
func (in *PulsarClusterSpec) setDefaults() (changed bool) {
	if in.PulsarVersion == "" {
		changed = true
		in.PulsarVersion = defaultImageTag
	}
	if in.ImagePullPolicy == "" {
		changed = true
		in.ImagePullPolicy = v1.PullIfNotPresent
	}
	if in.Size == nil {
		changed = true
		size := &defaultClusterSize
		in.Size = size
	}
	if in.ConfigurationStoreServers == "" {
		in.ConfigurationStoreServers = in.ZookeeperServers
	}
	if in.MaxUnavailableNodes < 0 {
		changed = true
		in.MaxUnavailableNodes = 1
	}
	if in.ClusterDomain == "" {
		changed = true
		in.ClusterDomain = defaultClusterDomain
	}
	if in.BrokerConfig == nil {
		in.BrokerConfig = map[string]string{}
	}
	if in.Ports == nil {
		in.Ports = &Ports{}
		in.Ports.setDefaults()
		changed = true
	} else if in.Ports.setDefaults() {
		changed = true
	}
	if in.KOP.PlainTextPort == 0 {
		changed = true
		in.KOP.PlainTextPort = defaultKopPlainPort
	}
	if in.KOP.SecuredPort == 0 {
		changed = true
		in.KOP.SecuredPort = defaultKopSSLPort
	}
	if in.Connectors.Builtin == nil {
		changed = true
		in.Connectors.Builtin = make([]string, 0)
		in.Connectors.Custom = make([]CustomConnectorSource, 0)
	}
	if in.ProbeConfig == nil {
		changed = true
		in.ProbeConfig = &pod.Probes{}
		in.ProbeConfig.SetDefault()
	} else if in.ProbeConfig.SetDefault() {
		changed = true
	}
	if in.JVMOptions.setDefaults() {
		changed = true
	}
	if in.Persistence == nil {
		changed = true
		in.Persistence = &v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(defaultBrokerPersistenceSize),
				},
			},
		}
	}
	if in.PodConfig.Spec.TerminationGracePeriodSeconds == nil {
		changed = true
		in.PodConfig.Spec.TerminationGracePeriodSeconds = &defaultTerminationGracePeriod
	}
	return changed
}

func (in *PulsarClusterSpec) VersionInt() int {
	if in.PulsarVersion == "" || in.PulsarVersion == "latest" {
		return math.MaxInt32
	}
	vsn := strings.ReplaceAll(in.PulsarVersion, ".", "")
	v, err := strconv.ParseInt(vsn, 10, 64)
	if err != nil {
		return math.MaxInt32
	}
	return int(v)
}

func (in *PulsarClusterSpec) createAnnotations() map[string]string {
	return in.Annotations
}

func (in *PulsarClusterSpec) createLabels(clusterName string, broker bool) map[string]string {
	labels := in.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	if broker {
		labels["broker"] = "true"
	}
	labels["app"] = "pulsar"
	labels[k8s.LabelAppName] = "pulsar"
	labels[k8s.LabelAppInstance] = clusterName
	labels[k8s.LabelAppManagedBy] = internal.OperatorName
	return labels
}
