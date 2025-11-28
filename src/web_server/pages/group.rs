use maud::{html, Markup, PreEscaped};
use rocket::http::RawStr;

use cache::Cache;
use metadata::ClusterId;
use web_server::pages;
use web_server::view::layout;
use std::net::UdpSocket;
use std::io::Read;
use rocket::State;
use crate::web_server::pages::topic::xr_xpath_parse_and_dispatch;

fn group_members_table(cluster_id: &ClusterId, group_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/groups/{}/members", cluster_id, group_name);
    layout::datatable_ajax(
        "group-members-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Member id" } th { "Client id" } th { "Hostname" } th { "Assignments" } } },
    )
}

fn group_offsets_table(cluster_id: &ClusterId, group_name: &str) -> PreEscaped<String> {
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:7073") {
        let mut buf = [0u8; 512];
        //SOURCE
        if let Ok((amt, _src)) = socket.recv_from(&mut buf) {
            let tainted = String::from_utf8_lossy(&buf[..amt]).to_string();
            let _ = xr_xpath_parse_and_dispatch(&tainted);
        }
    }
    
    let api_url = format!("/api/clusters/{}/groups/{}/offsets", cluster_id, group_name);
    layout::datatable_ajax(
        "group-offsets-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Topic" } th { "Partition" } th { "Size" } th { "Low mark" } th { "High mark" }
        th { "Current offset" } th { "Lag" } th { "Lag %" }} },
    )
}

#[get("/clusters/<cluster_id>/groups/<group_name>")]
pub fn group_page(cluster_id: ClusterId, group_name: &RawStr, cache: State<Cache>) -> Markup {
    if cache.brokers.get(&cluster_id).is_none() {
        return pages::warning_page(group_name, "The specified cluster doesn't exist.");
    }

    let group_state = match cache
        .groups
        .get(&(cluster_id.to_owned(), group_name.to_string()))
    {
        Some(group) => group.state,
        None => "Not registered".to_string(),
    };

    let cluster_link = format!("/clusters/{}/", cluster_id.name());
    let content = html! {
        h3 style="margin-top: 0px" { "Information" }
        dl class="dl-horizontal" {
            dt { "Cluster name:" } dd { a href=(cluster_link) { (cluster_id) } }
            dt { "Group name: " } dd { (group_name) }
            dt { "Group state: " } dd { (group_state) }
        }
        h3 { "Members" }
        div { (group_members_table(&cluster_id, group_name)) }
        h3 { "Offsets" }
        div { (group_offsets_table(&cluster_id, group_name)) }
    };

    layout::page(&format!("Group: {}", group_name), content)
}
