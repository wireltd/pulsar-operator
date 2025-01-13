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
	"fmt"
	"github.com/monimesl/operator-helper/oputil"
	v1 "k8s.io/api/core/v1"
	"log"
	"strings"
)

const (
	pulsarConfigEnvPrefix = "PULSAR_PREFIX_"
)

var notAllowedVariables = addPulsarEnvPrefix([]string{
	"statusFilePath", "clusterName", "zookeeperServers",
	"configurationStoreServers", "bookkeeperMetadataServiceUri",
	"PULSAR_GC", "PULSAR_MEM", "PULSAR_EXTRA_OPTS", "PULSAR_GC_LOG",
})

func processEnvVars(envs []v1.EnvVar) []v1.EnvVar {
	newEnvs := make([]v1.EnvVar, 0)
	for i := range envs {
		env := envs[i]
		name := env.Name
		actual := name
		if !strings.HasPrefix(name, pulsarConfigEnvPrefix) {
			name = fmt.Sprintf("%s%s", pulsarConfigEnvPrefix, name)
		}
		if oputil.Contains(notAllowedVariables, name) {
			log.Printf("ignoring the config: %s", actual)
			continue
		}
		env.Name = name
		newEnvs[i] = env
		newEnvs = append(newEnvs, env)
	}
	return newEnvs
}

func processEnvVarMap(envs map[string]string, ignoreNotAllowedVars bool) map[string]string {
	newEnvs := map[string]string{}
	for name, v := range envs {
		actual := name
		if !strings.HasPrefix(name, pulsarConfigEnvPrefix) {
			name = fmt.Sprintf("%s%s", pulsarConfigEnvPrefix, name)
		}
		if ignoreNotAllowedVars && oputil.Contains(notAllowedVariables, name) {
			log.Printf("ignoring the config: %s", actual)
			continue
		}
		newEnvs[name] = v
	}
	return newEnvs
}

func addPulsarEnvPrefix(envs []string) []string {
	newEnvs := make([]string, len(envs))
	for i := range envs {
		env := envs[i]
		if !strings.HasPrefix(env, pulsarConfigEnvPrefix) {
			env = fmt.Sprintf("%s%s", pulsarConfigEnvPrefix, env)
		}
		newEnvs[i] = env
	}
	return envs
}

func mergeLabels(ms ...map[string]string) map[string]string {
	res := make(map[string]string)
	for _, m := range ms {
		for key, value := range m {
			res[key] = value
		}
	}
	return res
}

func mapEqual(m1, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if m2[k] != v {
			return false
		}
	}
	return true
}
