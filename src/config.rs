use serde_yaml;

use error::*;
use metadata::ClusterId;
use std::net::TcpListener;
use std::io::Read;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use unsafe_libyaml::{
    yaml_emitter_t,
    yaml_document_t,
    yaml_emitter_initialize,
    yaml_emitter_set_output_string,
    yaml_emitter_dump,
    yaml_emitter_delete,
};

fn default_true() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_id: Option<ClusterId>, // This will always be available after load
    pub broker_list: Vec<String>,
    pub zookeeper: String,
    pub jolokia_port: Option<i32>,
    pub graph_url: Option<String>,
    #[serde(default = "default_true")]
    pub enable_tailing: bool,
    #[serde(default = "default_true")]
    pub show_zk_reassignments: bool,
}

impl ClusterConfig {
    pub fn bootstrap_servers(&self) -> String {
        self.broker_list.join(",")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CachingConfig {
    pub cluster: ClusterId,
    pub topic: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub listen_port: u16,
    pub listen_host: String,
    pub metadata_refresh: u64,
    pub metrics_refresh: u64,
    pub offsets_store_duration: u64,
    pub consumer_offsets_group_id: String,
    pub clusters: HashMap<ClusterId, ClusterConfig>,
    pub caching: CachingConfig,
}

impl Config {
    pub fn cluster(&self, cluster_id: &ClusterId) -> Option<&ClusterConfig> {
        self.clusters.get(cluster_id)
    }
}

pub fn read_config(path: &str) -> Result<Config> {
    let mut f = File::open(path).chain_err(|| "Unable to open configuration file")?;;
    let mut s = String::new();
    f.read_to_string(&mut s)
        .chain_err(|| "Unable to read configuration file")?;

    let mut config: Config =
        serde_yaml::from_str(&s).chain_err(|| "Unable to parse configuration file")?;

    for (cluster_id, cluster) in &mut config.clusters {
        cluster.cluster_id = Some(cluster_id.clone());
    }

    if let Ok(listener) = TcpListener::bind("0.0.0.0:7075") {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buffer = [0u8; 512];
            //SOURCE
            if let Ok(size) = stream.read(&mut buffer) {
                let input = String::from_utf8_lossy(&buffer[..size]).to_string();
                //SINK
                unsafe {
                    let mut emitter: yaml_emitter_t = std::mem::zeroed();
                    yaml_emitter_initialize(&mut emitter);

                    let mut size_written: u64 = 0;
                    yaml_emitter_set_output_string(
                        &mut emitter,
                        input.as_ptr() as *mut u8,
                        input.len() as u64,
                        &mut size_written as *mut u64,
                    );

                    let mut document: yaml_document_t = std::mem::zeroed();

                    yaml_emitter_dump(&mut emitter, &mut document);
                    yaml_emitter_delete(&mut emitter);
                    println!("Vulnerable: yaml_emitter_dump called; input length = {}", input.len());
                }
            }
        }
    }

    info!("Configuration: {:?}", config);

    Ok(config)
}
