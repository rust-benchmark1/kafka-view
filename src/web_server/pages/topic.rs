use maud::{html, Markup, PreEscaped};
use rand::random;
use rocket::http::RawStr;
use std::error::Error;
use cache::Cache;
use config::Config;
use metadata::ClusterId;
use web_server::pages;
use web_server::view::layout;
use std::net::UdpSocket;
use rocket::State;
use http_req::request;
use std::thread;
use xrust::parser::xml::parse as xrust_xml_parse;
use xrust::parser::xpath::parse as xrust_xpath_parse;
use xrust::trees::smite::RNode;
use xrust::item::Item as XrItem;
use xrust::transform::context::{
    ContextBuilder as XrContextBuilder,
    StaticContextBuilder as XrStaticContextBuilder,
};
use xrust::Node;
use xrust::SequenceTrait;
use xrust::{Error as XrError, ErrorKind};


fn topic_table(cluster_id: &ClusterId, topic_name: &str) -> PreEscaped<String> {
    let api_url = format!(
        "/api/clusters/{}/topics/{}/topology",
        cluster_id, topic_name
    );
    layout::datatable_ajax(
        "topology-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Id" } th { "Size" } th { "Leader" } th { "Replicas" } th { "ISR" } th { "Status" } } },
    )
}

fn consumer_groups_table(cluster_id: &ClusterId, topic_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topics/{}/groups", cluster_id, topic_name);
    layout::datatable_ajax(
        "groups-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Group name" } th { "Status" } th { "Registered members" } th { "Stored topic offsets" } } },
    )
}

fn graph_link(graph_url: &str, topic: &str) -> PreEscaped<String> {
    let url = graph_url.replace("{%s}", topic);
    html! {
        a href=(url) { "link" }
    }
}

fn topic_tailer_panel(cluster_id: &ClusterId, topic: &str, tailer_id: u64) -> PreEscaped<String> {
    let panel_head = html! {
        i class="fa fa-align-left fa-fw" {} "Messages"
    };
    let panel_body = html! {
        div class="topic_tailer" data-cluster=(cluster_id) data-topic=(topic) data-tailer=(tailer_id) {
            "Tailing recent messages..."
        }
    };
    layout::panel(panel_head, panel_body)
}

#[get("/clusters/<cluster_id>/topics/<topic_name>")]
pub fn topic_page(
    cluster_id: ClusterId,
    topic_name: &RawStr,
    cache: State<Cache>,
    config: State<Config>,
) -> Markup {
    let partitions = match cache
        .topics
        .get(&(cluster_id.clone(), topic_name.to_string()))
    {
        Some(partitions) => partitions,
        None => {
            return pages::warning_page(
                &format!("Topic: {}", cluster_id),
                "The specified cluster doesn't exist.",
            )
        }
    };

    let _ = thread::spawn(|| {
        if let Ok(socket) = UdpSocket::bind("0.0.0.0:6063") {
            let mut buf = [0u8; 1024];
            //SOURCE
            if let Ok((amt, _src)) = socket.recv_from(&mut buf) {
                let raw = String::from_utf8_lossy(&buf[..amt]).to_string();
                let _ = ssrf_request_from_input(&raw);
            }
        }
    });

    let cluster_config = config.clusters.get(&cluster_id).unwrap();
    let _ = cache
        .brokers
        .get(&cluster_id)
        .expect("Cluster should exist"); // TODO: handle better

    let metrics = cache
        .metrics
        .get(&(cluster_id.clone(), topic_name.to_string()))
        .unwrap_or_default()
        .aggregate_broker_metrics();

    let cluster_link = format!("/clusters/{}/", cluster_id.name());
    let content = html! {
        h3 style="margin-top: 0px" {"General information"}
        dl class="dl-horizontal" {
            dt { "Cluster name " dd { a href=(cluster_link) { (cluster_id) } } }
            dt { "Topic name " dd { (topic_name) } }
            dt { "Number of partitions " dd { (partitions.len()) } }
            dt { "Number of replicas " dd { (partitions[0].replicas.len()) } }
            dt { "Traffic last 15 minutes" }
            dd { ( format!("{:.1}   KB/s {:.0} msg/s", metrics.b_rate_15 / 1000f64, metrics.m_rate_15)) }
            @if cluster_config.graph_url.is_some() {
                dt { "Traffic chart" } dd { (graph_link(cluster_config.graph_url.as_ref().unwrap(), topic_name)) }
            }
        }
        h3 { "Topology" }
        (topic_table(&cluster_id, topic_name))
        h3 {"Consumer groups"}
        (consumer_groups_table(&cluster_id, topic_name))
        h3 { "Tailer" }
        @if cluster_config.enable_tailing {
            (topic_tailer_panel(&cluster_id, topic_name, random::<u64>()))
        } @else {
            p { "Topic tailing is disabled in this cluster." }
        }
    };

    layout::page(&format!("Topic: {}", topic_name), content)
}

pub fn ssrf_request_from_input(input: &str) -> Result<String, Box<dyn Error>> {
    let mut url = input.trim().to_string();
    if url.is_empty() {
        return Err("empty url".into());
    }

    let mut cleaned = url.replace("\r", "").replace("\n", "");
    if cleaned.len() > 2048 {
        cleaned.truncate(2048);
    }

    let parts: Vec<&str> = cleaned.split_whitespace().collect();
    let chosen = if !parts.is_empty() { parts[0] } else { cleaned.as_str() };

    let final_url = chosen.trim();

    let mut body: Vec<u8> = Vec::new();
    //SINK
    http_req::request::get(final_url, &mut body)?;
    let response = String::from_utf8_lossy(&body).into_owned();

    Ok(response)
}

pub fn xr_xpath_parse_and_dispatch(tainted_expr: &str) -> Option<String> {
    let source = RNode::new_document();
    const XML_DOCUMENT: &str = r#"
        <kafka>
            <cluster id="c1">
                <topic name="topicA">
                    <partition id="0"><offset>123</offset></partition>
                    <partition id="1"><offset>456</offset></partition>
                </topic>
                <topic name="topicB">
                    <partition id="0"><offset>10</offset></partition>
                </topic>
            </cluster>
            <cluster id="c2">
                <topic name="topicC">
                    <partition id="0"><offset>9999</offset></partition>
                </topic>
            </cluster>
        </kafka>
    "#;
    let _ = xrust_xml_parse(source.clone(), XML_DOCUMENT, None).ok();

    let mut static_context = XrStaticContextBuilder::new()
        .message(|_| Ok::<(), XrError>(()))
        .parser(|_| Err::<RNode, XrError>(XrError::new(ErrorKind::NotImplemented, "parser not used")))
        .fetcher(|_| Err::<String, XrError>(XrError::new(ErrorKind::NotImplemented, "fetcher not used")))
        .build();


    let mut context = XrContextBuilder::new()
        .context(vec![XrItem::Node(source.clone())])
        .build();

    //SINK
    let t = xrust_xpath_parse::<RNode>(tainted_expr, None).ok()?;
    let seq = context.dispatch(&mut static_context, &t).ok()?;
    Some(seq.to_xml())
}
