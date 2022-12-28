use crate::raft_dfs;
use crate::raft_dfs::{
    DataNodeConfig, DataNodeDeleteReq, DataNodeDeleteRes, DataNodeReadReq, DataNodeReadRes,
    DataNodeWriteReq, DataNodeWriteRes, NameNodeDeleteReq, NameNodeDeleteRes, NameNodeReadReq,
    NameNodeReadRes, NameNodeWriteReq, NameNodeWriteRes,
};
use raft_dfs::data_node_service_client::DataNodeServiceClient;
use raft_dfs::name_node_service_client::NameNodeServiceClient;
use tonic::transport::Uri;
use tonic::Response;
pub fn build_uri(str: String) -> Uri {
    str.parse::<Uri>().unwrap()
}

#[allow(dead_code)]
impl DataNodeConfig {
    pub fn default() -> DataNodeConfig {
        DataNodeConfig {
            id: "None".to_string(),
            addr: "None".to_string(),
        }
    }
}

pub async fn namenode_read_handler(
    filename: String,
) -> Result<Response<NameNodeReadRes>, Box<dyn std::error::Error>> {
    let mut client =
        NameNodeServiceClient::connect("http://[::1]:2333".parse::<Uri>().unwrap()).await?;
    let request = tonic::Request::new(NameNodeReadReq { filename: filename });
    let read_res = client.read(request).await;
    Ok::<Response<NameNodeReadRes>, Box<dyn std::error::Error>>(read_res?)
}

pub async fn datanode_read_handler(
    filename: String,
    leader_addr: String,
) -> Result<Response<DataNodeReadRes>, Box<dyn std::error::Error>> {
    let mut client = DataNodeServiceClient::connect(build_uri(leader_addr)).await?;
    let request = tonic::Request::new(DataNodeReadReq { filename: filename });
    let read_res = client.read(request).await;
    Ok::<Response<DataNodeReadRes>, Box<dyn std::error::Error>>(read_res?)
}

#[allow(dead_code)]
pub async fn read(filename: String) -> Result<String, Box<dyn std::error::Error>> {
    let namenode_res = namenode_read_handler(filename.clone()).await;
    let leader_addr = namenode_res.unwrap().into_inner().addr;
    let datanode_res = datanode_read_handler(filename.clone(), leader_addr.clone()).await;
    println!("leader_addr {}", leader_addr);
    let content = datanode_res.unwrap().into_inner().content;
    Ok::<String, Box<dyn std::error::Error>>(content)
}

pub async fn namenode_write_handler(
    filename: String,
    content: String,
) -> Result<Response<NameNodeWriteRes>, Box<dyn std::error::Error>> {
    let mut client =
        NameNodeServiceClient::connect("http://[::1]:2333".parse::<Uri>().unwrap()).await?;
    let request = tonic::Request::new(NameNodeWriteReq {
        filename: filename,
        content: content,
    });
    let write_res = client.write(request).await;
    Ok::<Response<NameNodeWriteRes>, Box<dyn std::error::Error>>(write_res?)
}

pub async fn datanode_write_handler(
    filename: String,
    content: String,
    leader_addr: String,
) -> Result<Response<DataNodeWriteRes>, Box<dyn std::error::Error>> {
    let mut client = DataNodeServiceClient::connect(build_uri(leader_addr)).await?;
    let request = tonic::Request::new(DataNodeWriteReq {
        filename: filename,
        content: content,
    });
    let write_res = client.write(request).await;
    Ok::<Response<DataNodeWriteRes>, Box<dyn std::error::Error>>(write_res?)
}

#[allow(dead_code)]
pub async fn write(filename: String, content: String) -> Result<(), Box<dyn std::error::Error>> {
    let namenode_res = namenode_write_handler(filename.clone(), content.clone()).await;
    let leader_addr = namenode_res.unwrap().into_inner().addr;
    let _ = datanode_write_handler(filename.clone(), content.clone(), leader_addr.clone()).await;
    Ok::<(), Box<dyn std::error::Error>>(())
}

pub async fn namenode_delete_handler(
    filename: String,
) -> Result<Response<NameNodeDeleteRes>, Box<dyn std::error::Error>> {
    let mut client =
        NameNodeServiceClient::connect("http://[::1]:2333".parse::<Uri>().unwrap()).await?;
    let request = tonic::Request::new(NameNodeDeleteReq { filename: filename });
    let delete_res = client.delete(request).await;
    Ok::<Response<NameNodeDeleteRes>, Box<dyn std::error::Error>>(delete_res?)
}

pub async fn datanode_delete_handler(
    filename: String,
    leader_addr: String,
) -> Result<Response<DataNodeDeleteRes>, Box<dyn std::error::Error>> {
    let mut client = DataNodeServiceClient::connect(build_uri(leader_addr)).await?;
    let request = tonic::Request::new(DataNodeDeleteReq { filename: filename });
    let delete_res = client.delete(request).await;
    Ok::<Response<DataNodeDeleteRes>, Box<dyn std::error::Error>>(delete_res?)
}

#[allow(dead_code)]
pub async fn delete(filename: String) -> Result<(), Box<dyn std::error::Error>> {
    let namenode_res = namenode_delete_handler(filename.clone()).await;
    let leader_addr = namenode_res.unwrap().into_inner().addr;
    let _ = datanode_delete_handler(filename.clone(), leader_addr.clone()).await;
    Ok::<(), Box<dyn std::error::Error>>(())
}


