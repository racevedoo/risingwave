// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::prelude::*;
use itertools::Itertools;

use crate::PrometheusConfig;

pub struct PrometheusGen;

impl PrometheusGen {
    pub fn gen_prometheus_yml(&self, config: &PrometheusConfig) -> String {
        let prometheus_host = &config.address;
        let prometheus_port = &config.port;
        let compute_node_targets = config
            .provide_compute_node
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.exporter_port))
            .join(",");

        let frontend_targets = config
            .provide_frontend
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.exporter_port))
            .join(",");

        let meta_node_targets = config
            .provide_meta_node
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.exporter_port))
            .join(",");

        let minio_targets = config
            .provide_minio
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.port))
            .join(",");

        let compactor_targets = config
            .provide_compactor
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.exporter_port))
            .join(",");

        let etcd_targets = config
            .provide_etcd
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.exporter_port))
            .join(",");

        let redpanda_targets = config
            .provide_redpanda
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, 9644))
            .join(",");

        let now = Local::now().format("%Y%m%d-%H%M%S");

        let remote_write = if config.remote_write {
            let remote_write_region = &config.remote_write_region;
            let remote_write_url = &config.remote_write_url;
            format!(
                r#"
remote_write:
  -
    url: {remote_write_url}
    queue_config:
        max_samples_per_send: 1000
        max_shards: 200
        capacity: 2500
    sigv4:
         region: {remote_write_region}
"#,
            )
        } else {
            String::from("")
        };

        format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
global:
  scrape_interval: 15s
  evaluation_interval: 60s
  external_labels:
    rw_cluster: {now}
{remote_write}

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["{prometheus_host}:{prometheus_port}"]

  - job_name: compute
    static_configs:
      - targets: [{compute_node_targets}]

  - job_name: meta
    static_configs:
      - targets: [{meta_node_targets}]
  
  - job_name: minio
    metrics_path: /minio/v2/metrics/cluster
    static_configs:
    - targets: [{minio_targets}]

  - job_name: compactor
    static_configs:
      - targets: [{compactor_targets}]

  - job_name: etcd
    static_configs:
      - targets: [{etcd_targets}]

  - job_name: frontend
    static_configs:
      - targets: [{frontend_targets}]

  - job_name: redpanda
    static_configs:
      - targets: [{redpanda_targets}]
"#,
        )
    }
}
