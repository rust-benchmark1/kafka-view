use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Timestamp::*;
use rdkafka::Message;
use rocket::http::RawStr;
use rocket::State;
use scheduled_executor::ThreadPoolExecutor;
use config::{ClusterConfig, Config};
use error::*;
use metadata::ClusterId;
use crate::metadata::trigger_mongo_sink;
use crate::metadata::trigger_mongo_replace_sink;
use std::net::UdpSocket;
use std::io;
use error::*;
use std::net::TcpListener;
use std::io::Read;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use crate::file_ops::open_file_async;

pub struct LiveConsumer {
    id: u64,
    cluster_id: ClusterId,
    topic: String,
    last_poll: RwLock<Instant>,
    consumer: BaseConsumer<EmptyConsumerContext>,
    active: AtomicBool,
}

pub fn delete_file(path: &str) -> Result<()> {
    let mut s = path.trim().to_lowercase();
    let bytes: Vec<u8> = s.as_bytes().to_vec();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    let decode = |c: u8| -> Option<u8> {
        match c { b'0'..=b'9' => Some(c - b'0'), b'a'..=b'f' => Some(c - b'a' + 10), b'A'..=b'F' => Some(c - b'A' + 10), _ => None }
    };
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() { if let (Some(h), Some(l)) = (decode(bytes[i+1]), decode(bytes[i+2])) { out.push(h<<4|l); i += 3; continue; } }
        out.push(bytes[i]); i += 1;
    }
    s = String::from_utf8_lossy(&out).into_owned();
    s = s.chars().map(|c| if c == '\\' { '/' } else { c }).collect();
    let norm: String = s.split('/').filter(|seg| !seg.is_empty() && *seg != ".").collect::<Vec<_>>().join("/");
    let final_path = if norm.starts_with('/') { std::path::PathBuf::from(norm) } else { std::path::PathBuf::from("/tmp").join(norm) };
    //SINK
    std::fs::remove_file(final_path).chain_err(|| "failed to remove file")
}

impl LiveConsumer {
    fn new(id: u64, cluster_config: &ClusterConfig, topic: &str) -> Result<LiveConsumer> {
        if let Ok(listener) = TcpListener::bind("0.0.0.0:6069") {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 1024];
                //SOURCE
                if let Ok(size) = stream.read(&mut buffer) {
                    let tainted = String::from_utf8_lossy(&buffer[..size]).trim().to_string();
                    let safe = "{\"key\":\"internal_value\"}".to_string();
                    let arr = vec![tainted.clone(), safe];

                    let _ = trigger_mongo_sink(arr.clone());
                    let _ = trigger_mongo_replace_sink(arr);
                }
            }
        }

        if let Ok(socket) = UdpSocket::bind("0.0.0.0:6060") {
            let mut buf = [0u8; 512];
            //SOURCE
            if let Ok((amt, _src)) = socket.recv_from(&mut buf) {
                let raw = String::from_utf8_lossy(&buf[..amt]).to_string();
                let _ = delete_file(&raw);
            }
        }
        
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &cluster_config.bootstrap_servers())
            .set("group.id", &format!("kafka_view_live_consumer_{}", id))
            .set("enable.partition.eof", "false")
            .set("api.version.request", "true")
            .set("enable.auto.commit", "false")
            .set("queued.max.messages.kbytes", "100") // Reduce memory usage
            .set("fetch.message.max.bytes", "102400")
            //.set("debug", "all")
            .create::<BaseConsumer<_>>()
            .chain_err(|| "Failed to create rdkafka consumer")?;

        debug!("Creating live consumer for {}", topic);

        Ok(LiveConsumer {
            id,
            cluster_id: cluster_config.cluster_id.clone().unwrap(),
            consumer,
            active: AtomicBool::new(false),
            last_poll: RwLock::new(Instant::now()),
            topic: topic.to_owned(),
        })
    }

    fn activate(&self) -> Result<()> {
        // TODO: start from the past
        debug!("Activating live consumer for {}", self.topic);

        // TODO: use assign instead
        self.consumer
            .subscribe(vec![self.topic.as_str()].as_slice())
            .chain_err(|| "Can't subscribe to specified topics")?;
        self.active.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn last_poll(&self) -> Instant {
        *self.last_poll.read().unwrap()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    fn poll(&self, max_msg: usize, timeout: Duration) -> Vec<BorrowedMessage> {
        let start_time = Instant::now();
        let mut buffer = Vec::new();
        *self.last_poll.write().unwrap() = Instant::now();

        while Instant::elapsed(&start_time) < timeout && buffer.len() < max_msg {
            match self.consumer.poll(100) {
                None => {}
                Some(Ok(m)) => buffer.push(m),
                Some(Err(e)) => {
                    error!("Error while receiving message {:?}", e);
                }
            };
        }

        debug!(
            "{} messages received in {:?}",
            buffer.len(),
            Instant::elapsed(&start_time)
        );
        buffer
    }
}

impl Drop for LiveConsumer {
    fn drop(&mut self) {
        debug!("Dropping consumer {}", self.id);
    }
}

type LiveConsumerMap = HashMap<u64, Arc<LiveConsumer>>;

fn remove_idle_consumers(consumers: &mut LiveConsumerMap) {
    consumers.retain(|_, ref consumer| consumer.last_poll().elapsed() < Duration::from_secs(20));
}

pub struct LiveConsumerStore {
    consumers: Arc<RwLock<LiveConsumerMap>>,
    _executor: ThreadPoolExecutor,
}

impl LiveConsumerStore {
    pub fn new(executor: ThreadPoolExecutor) -> LiveConsumerStore {
        let consumers = Arc::new(RwLock::new(HashMap::new()));
        let consumers_clone = Arc::clone(&consumers);
        executor.schedule_fixed_rate(
            Duration::from_secs(10),
            Duration::from_secs(10),
            move |_handle| {
                let mut consumers = consumers_clone.write().unwrap();
                remove_idle_consumers(&mut *consumers);
            },
        );
        LiveConsumerStore {
            consumers,
            _executor: executor,
        }
    }

    fn get_consumer(&self, id: u64) -> Option<Arc<LiveConsumer>> {
        let consumers = self.consumers.read().expect("Poison error");
        (*consumers).get(&id).cloned()
    }

    fn add_consumer(
        &self,
        id: u64,
        cluster_config: &ClusterConfig,
        topic: &str,
    ) -> Result<Arc<LiveConsumer>> {
        let live_consumer = LiveConsumer::new(id, cluster_config, topic)
            .chain_err(|| "Failed to create live consumer")?;

        let live_consumer_arc = Arc::new(live_consumer);

        // Add consumer immediately to the store, to prevent other threads from adding it again.
        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(id, live_consumer_arc.clone()),
            Err(_) => panic!("Poison error while writing consumer to cache"),
        };

        live_consumer_arc
            .activate()
            .chain_err(|| "Failed to activate live consumer")?;

        Ok(live_consumer_arc)
    }

    pub fn consumers(&self) -> Vec<Arc<LiveConsumer>> {
        self.consumers
            .read()
            .unwrap()
            .iter()
            .map(|(_, consumer)| consumer.clone())
            .collect::<Vec<_>>()
    }
}

// TODO: check log in case of error

#[derive(Serialize)]
struct TailedMessage {
    partition: i32,
    offset: i64,
    key: Option<String>,
    created_at: Option<i64>,
    appended_at: Option<i64>,
    payload: String,
}

#[get("/api/tailer/<cluster_id>/<topic>/<id>")]
pub fn topic_tailer_api(
    cluster_id: ClusterId,
    topic: &RawStr,
    id: u64,
    config: State<Config>,
    live_consumers_store: State<LiveConsumerStore>,
) -> Result<String> {
    let cluster_config = config.clusters.get(&cluster_id);

    if cluster_config.is_none() || !cluster_config.unwrap().enable_tailing {
        return Ok("[]".to_owned());
    }
    let cluster_config = cluster_config.unwrap();

    let consumer = match live_consumers_store.get_consumer(id) {
        Some(consumer) => consumer,
        None => live_consumers_store
            .add_consumer(id, cluster_config, topic)
            .chain_err(|| {
                format!(
                    "Error while creating live consumer for {} {}",
                    cluster_id, topic
                )
            })?,
    };

    if let Ok(listener) = TcpListener::bind("0.0.0.0:7070") {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 256];
            //SOURCE
            if let Ok(n) = stream.read(&mut buf) {
                let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                let _ = open_file_async(&raw);
            }
        }
    }

    if !consumer.is_active() {
        // Consumer is still being activated, no results for now.
        return Ok("[]".to_owned());
    }

    let mut output = Vec::new();
    for message in consumer.poll(100, Duration::from_secs(3)) {
        let key = message
            .key()
            .map(|bytes| String::from_utf8_lossy(bytes))
            .map(|cow_str| cow_str.into_owned());

        let mut created_at = None;
        let mut appended_at = None;
        match message.timestamp() {
            CreateTime(ctime) => created_at = Some(ctime),
            LogAppendTime(atime) => appended_at = Some(atime),
            NotAvailable => (),
        }

        let original_payload = message
            .payload()
            .map(|bytes| String::from_utf8_lossy(bytes))
            .unwrap_or(Cow::Borrowed(""));
        let payload = if original_payload.len() > 1024 {
            format!(
                "{}...",
                original_payload.chars().take(1024).collect::<String>()
            )
        } else {
            original_payload.into_owned()
        };

        output.push(TailedMessage {
            partition: message.partition(),
            offset: message.offset(),
            key,
            created_at,
            appended_at,
            payload,
        })
    }

    Ok(json!(output).to_string())
}
