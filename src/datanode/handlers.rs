// #[tonic::async_trait]
use crate::raft_dfs::data_node_service_client::DataNodeServiceClient;
use crate::raft_dfs::name_node_service_client::NameNodeServiceClient;
use crate::raft_dfs::{
    AppendEntriesReq, AppendEntriesRes, DataNodeConfig, DataNodeUpdateAddrReq,
    DataNodeUpdateAddrRes, NameNodeUpdateAddrReq, NameNodeUpdateAddrRes, RegisterReq, RegisterRes,
    RequestVoteReq, RequestVoteRes, UpdateLeaderReq, UpdateLeaderRes, UpdateLogReq, UpdateLogRes,
};
use crate::utils::build_uri;
use tonic::{transport::Uri, Response };

pub async fn update_namenode_leader_handler(
    id: String,
    term: i32,
) -> Result<Response<UpdateLeaderRes>, Box<dyn std::error::Error>> {
    let mut client =
        NameNodeServiceClient::connect(build_uri("http://[::1]:2333".to_string())).await?;

    let message = UpdateLeaderReq { id: id, term: term };
    let request = tonic::Request::new(message);
    let res = client.update_leader(request).await;
    Ok::<Response<UpdateLeaderRes>, Box<dyn std::error::Error>>(res?)
}

pub async fn update_namenode_addr_handler(
    message: String,
    nodes: Vec<DataNodeConfig>,
) -> Result<Response<NameNodeUpdateAddrRes>, Box<dyn std::error::Error>> {
    let mut client =
        NameNodeServiceClient::connect(build_uri("http://[::1]:2333".to_string())).await?;
    let req = NameNodeUpdateAddrReq {
        message: message,
        nodes: nodes,
    };
    let request = tonic::Request::new(req);
    let res = client.update_addr(request).await;
    Ok::<Response<NameNodeUpdateAddrRes>, Box<dyn std::error::Error>>(res?)
}

pub async fn update_peers_log_handler(
    peer_addr: String,
    id: String,
    term: i32,
    logs: Vec<String>,
) -> Result<Response<UpdateLogRes>, Box<dyn std::error::Error>> {
    let mut client = DataNodeServiceClient::connect(build_uri(peer_addr)).await?;
    let message = UpdateLogReq {
        id: id,
        term: term,
        logs: logs,
    };
    let request = tonic::Request::new(message);
    let res = client.update_log(request).await;
    Ok::<Response<UpdateLogRes>, Box<dyn std::error::Error>>(res?)
}

pub async fn update_peers_addr_handler(
    message: String,
    peer_addr: String,
    nodes: Vec<DataNodeConfig>,
) -> Result<Response<DataNodeUpdateAddrRes>, Box<dyn std::error::Error>> {
    let mut client = DataNodeServiceClient::connect(build_uri(peer_addr)).await?;
    let req = DataNodeUpdateAddrReq {
        message: message,
        nodes: nodes,
    };
    let request = tonic::Request::new(req);
    let res = client.update_addr(request).await;
    Ok::<Response<DataNodeUpdateAddrRes>, Box<dyn std::error::Error>>(res?)
}

pub async fn request_vote_handler(
    peer_addr: String,
    vote_term: i32,
    vote_id: String,
) -> Result<Response<RequestVoteRes>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = DataNodeServiceClient::connect(build_uri(peer_addr.to_string())).await?;

    let message = RequestVoteReq {
        term: vote_term,
        candidate_id: vote_id.clone(),
    };
    let request = tonic::Request::new(message);
    let res = client.request_vote(request).await;
    Ok::<Response<RequestVoteRes>, Box<dyn std::error::Error + Send + Sync>>(res?)
}

pub async fn namenode_register_handler(
    id: String,
    addr: String,
) -> Result<Response<RegisterRes>, Box<dyn std::error::Error>> {
    let mut client =
        NameNodeServiceClient::connect("http://[::1]:2333".parse::<Uri>().unwrap()).await?;

    let message = RegisterReq { id: id, addr: addr };
    let request = tonic::Request::new(message);
    let res = client.register(request).await;
    Ok::<Response<RegisterRes>, Box<dyn std::error::Error>>(res?)
}

pub async fn send_heartbeat_handler(
    addr: String,
    term: i32,
    id: String,
    entries: Vec<String>,
    index: i32,
) -> Result<Response<AppendEntriesRes>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = DataNodeServiceClient::connect(build_uri(addr.to_string())).await?;
    let message = AppendEntriesReq {
        term: term,
        id: id,
        entries: entries,
        index: index,
    };
    let request = tonic::Request::new(message);
    let res = client.append_entries(request).await;
    Ok::<Response<AppendEntriesRes>, Box<dyn std::error::Error + Send + Sync>>(res?)
}
