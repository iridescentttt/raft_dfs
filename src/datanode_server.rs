use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use tonic::transport::{Server};

mod datanode;
mod raft_dfs;
mod utils;
use datanode::{background_task, register_task, DataNode};
use raft_dfs::data_node_service_server::DataNodeServiceServer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = String::from(&args[1]);
    let port = String::from(&args[2]);
    let mut path = String::from(&args[3]);
    if path.chars().last().unwrap() != '/' {
        path.push_str("/");
    }
    let addr = format!("[::1]:{}", port).parse()?;
    let addr_str = format!("http://[::1]:{}", port);
    let node = Arc::new(Mutex::new(DataNode::builder(id, path, addr_str)));
    
    let register_node =Arc::clone(&node);
    register_task(register_node);
    let background_node = Arc::clone(&node);
    let _ = thread::spawn(move || {
        background_task(background_node);
    });

    let service_node = Arc::clone(&node);
    Server::builder()
        .add_service(DataNodeServiceServer::from_arc(service_node))
        .serve(addr)
        .await?;

    Ok(())
}
