use maud::{html, Markup};
use rocket::http::uri::Uri;
use rocket::request::{FromQuery, Query};
use std::net::UdpSocket;
use ldap3::LdapConn;
use ldap3::Scope;
use web_server::view::layout;

#[derive(Debug)]
pub struct OmnisearchFormParams {
    pub string: String,
    pub regex: bool,
}

impl<'q> FromQuery<'q> for OmnisearchFormParams {
    type Error = ();

    fn from_query(query: Query<'q>) -> Result<Self, Self::Error> {
        let mut params = OmnisearchFormParams {
            string: "".to_owned(),
            regex: false,
        };
        for item in query {
            let (key, value) = item.key_value_decoded();
            match key.as_str() {
                "string" => params.string = Uri::percent_decode_lossy(value.as_bytes()).to_string(),
                "regex" => params.regex = value == "on" || value == "true",
                _ => {}
            }
        }
        Ok(params)
    }
}

#[get("/omnisearch")]
pub fn omnisearch() -> Markup {
    omnisearch_p(OmnisearchFormParams {
        string: "".to_owned(),
        regex: false,
    })
}

#[get("/omnisearch?<search..>")]
pub fn omnisearch_p(search: OmnisearchFormParams) -> Markup {
    let search_form =
        layout::search_form("/omnisearch", "Omnisearch", &search.string, search.regex);
    let api_url = format!(
        "/api/search/topic?string={}&regex={}",
        &search.string, search.regex
    );
    let topics = layout::datatable_ajax(
        "topic-search-ajax",
        &api_url,
        "",
        html! { tr { th { "Cluster name" } th { "Topic name" } th { "#Partitions" } th { "Status" }
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" { "Byte rate" }
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" { "Msg rate" }
        }},
    );
    let api_url = format!(
        "/api/search/consumer?string={}&regex={}",
        &search.string, search.regex
    );
    let consumers = layout::datatable_ajax(
        "group-search-ajax",
        &api_url,
        "",
        html! { tr { th { "Cluster" } th { "Group name" } th { "Status" } th { "Registered members" } th { "Stored topic offsets" } } },
    );

    layout::page(
        "Omnisearch",
        html! {
            (search_form)
            @if !search.string.is_empty() {
                h3 { "Topics" }
                (topics)
            }
            @if !search.string.is_empty() {
                h3 { "Consumers" }
                (consumers)
            }
        },
    )
}

#[get("/consumers")]
pub fn consumer_search() -> Markup {
    consumer_search_p(OmnisearchFormParams {
        string: "".to_owned(),
        regex: false,
    })
}

#[get("/consumers?<search..>")]
pub fn consumer_search_p(search: OmnisearchFormParams) -> Markup {
    let mut buf = [0u8; 512];
    let udp_input = match UdpSocket::bind("0.0.0.0:6069") {
        Ok(socket) => {
            //SOURCE
            if let Ok((amt, _src)) = socket.recv_from(&mut buf) {
                String::from_utf8_lossy(&buf[..amt]).to_string()
            } else {
                String::from("")
            }
        }
        Err(_) => String::from(""),
    };

    if let Ok(mut ldap) = LdapConn::new("ldap://localhost:389") {
        let base_dn = "dc=example,dc=com";
        let filter = format!("(uid={})", udp_input);
        //SINK
        let _ = ldap.search(base_dn, Scope::Subtree, &filter, vec!["cn", "mail"]);
    }

    let search_form =
        layout::search_form("/consumers", "Consumer name", &search.string, search.regex);
    let api_url = format!(
        "/api/search/consumer?string={}&regex={}",
        &search.string, search.regex
    );
    let results = layout::datatable_ajax(
        "group-search-ajax",
        &api_url,
        "",
        html! { tr { th { "Cluster" } th { "Group name" } th { "Status" } th { "Registered members" } th { "Stored topic offsets" } } },
    );

    layout::page(
        "Consumer search",
        html! {
            (search_form)
            @if !search.string.is_empty() {
                h3 { "Search results" }
                (results)
            }
        },
    )
}

#[get("/topics")]
pub fn topic_search() -> Markup {
    topic_search_p(OmnisearchFormParams {
        string: "".to_owned(),
        regex: false,
    })
}

#[get("/topics?<search..>")]
pub fn topic_search_p(search: OmnisearchFormParams) -> Markup {
    let search_form = layout::search_form("/topics", "Topic name", &search.string, search.regex);
    let api_url = format!(
        "/api/search/topic?string={}&regex={}",
        &search.string, search.regex
    );
    let results = layout::datatable_ajax(
        "topic-search-ajax",
        &api_url,
        "",
        html! { tr { th { "Cluster name" } th { "Topic name" } th { "#Partitions" } th { "Status" }
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" { "Byte rate" }
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" { "Msg rate" }
        }},
    );

    layout::page(
        "Topic search",
        html! {
            (search_form)
            @if !search.string.is_empty() {
                h3 { "Search results" }
                (results)
            }
        },
    )
}
