use std::sync::{Arc, Mutex};
use std::{env, thread};
use tonic::transport::Server;

mod namenode;
mod raft_dfs;
mod utils;
use namenode::{background_task, NameNode};
use raft_dfs::name_node_service_server::NameNodeServiceServer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let port = String::from(&args[1]);

    let addr = format!("[::1]:{}", port).parse()?;
    let addr_str = format!("[::1]:{}", port);
    let node = Arc::new(Mutex::new(NameNode::builder(addr_str)));

    let background_node = Arc::clone(&node);
    let _ = thread::spawn(move || {
        background_task(background_node);
    });

    let service_node = Arc::clone(&node);
    Server::builder()
        .add_service(NameNodeServiceServer::from_arc(service_node))
        .serve(addr)
        .await?;

    Ok(())
}
