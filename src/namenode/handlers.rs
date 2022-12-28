use crate::raft_dfs::data_node_service_client::DataNodeServiceClient;
use crate::raft_dfs::{
    DataNodeConfig, DataNodeUpdateAddrReq,
    DataNodeUpdateAddrRes, 
};
use crate::utils::build_uri;
use tonic::{ Response };

pub async fn update_addr_handler(
    register_node: DataNodeConfig,
    addr: String,
    nodes: Vec<DataNodeConfig>,
) -> Result<Response<DataNodeUpdateAddrRes>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = DataNodeServiceClient::connect(build_uri(addr)).await?;
    let message = format!(
        "new node register\n-> id: {}, addr: {}",
        register_node.id, register_node.addr
    );
    let req = DataNodeUpdateAddrReq {
        message: message,
        nodes: nodes,
    };
    let request = tonic::Request::new(req);
    let res = client.update_addr(request).await;
    Ok::<Response<DataNodeUpdateAddrRes>, Box<dyn std::error::Error + Send + Sync>>(res?)
}
