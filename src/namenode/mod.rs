use chrono::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{str, thread};
use tonic::{Request, Response, Status};

use crate::raft_dfs::name_node_service_server::NameNodeService;
use crate::raft_dfs::{
    DataNodeConfig, NameNodeDeleteReq, NameNodeDeleteRes, NameNodeReadReq, NameNodeReadRes,
    NameNodeUpdateAddrReq, NameNodeUpdateAddrRes, NameNodeWriteReq, NameNodeWriteRes, RegisterReq,
    RegisterRes, UpdateLeaderReq, UpdateLeaderRes,
};

use self::handlers::update_addr_handler;

pub mod file_service;
pub mod handlers;
pub mod name_service;
#[derive(Debug, PartialEq)]
pub enum LogLevel {
    FileLog,
    SystemLog,
}

#[derive(Clone)]
#[derive()]
pub struct NameNode {
    pub addr: String,
    pub nodes: HashMap<String, String>,
    pub file_logs: Vec<String>,
    pub sys_logs: Vec<String>,
    pub leader_cfg: Option<DataNodeConfig>,
    pub term: i32,
}

impl NameNode {
    pub fn builder(addr: String) -> NameNode {
        NameNode {
            term: 0,
            addr: addr,
            nodes: HashMap::new(),
            sys_logs: Vec::new(),
            file_logs: Vec::new(),
            leader_cfg: Some(DataNodeConfig::default()),
        }
    }

    fn build_log(&self, message: &str) -> String {
        String::from(format!(
            "[{}] {}",
            chrono::offset::Local::now()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            message
        ))
    }
    fn dump_log(&mut self, message: &str, level: LogLevel) -> () {
        let log = self.build_log(message);
        match level {
            LogLevel::FileLog => self.file_logs.push(log.clone()),
            LogLevel::SystemLog => self.sys_logs.push(log.clone()),
        }
        println!("{}", log);
    }
}

#[tonic::async_trait]
impl NameNodeService for Mutex<NameNode> {
    async fn read(
        &self,
        request: Request<NameNodeReadReq>,
    ) -> Result<Response<NameNodeReadRes>, Status> {
        self.lock().unwrap().handle_read(request)
    }
    async fn write(
        &self,
        request: Request<NameNodeWriteReq>,
    ) -> Result<Response<NameNodeWriteRes>, Status> {
        self.lock().unwrap().handle_write(request)
    }
    async fn delete(
        &self,
        request: Request<NameNodeDeleteReq>,
    ) -> Result<Response<NameNodeDeleteRes>, Status> {
        self.lock().unwrap().handle_delete(request)
    }
    async fn register(
        &self,
        request: Request<RegisterReq>,
    ) -> Result<Response<RegisterRes>, Status> {
        let (res, register_node, nodes_addr, nodes) = self.lock().unwrap().handle_register(request);
        for (_, node_addr) in nodes_addr.iter() {
            let _ =
                update_addr_handler(register_node.clone(), node_addr.to_string(), nodes.to_vec())
                    .await;
        }
        res
    }
    async fn update_leader(
        &self,
        request: Request<UpdateLeaderReq>,
    ) -> Result<Response<UpdateLeaderRes>, Status> {
        self.lock().unwrap().handle_update_leader(request)
    }
    async fn update_addr(
        &self,
        request: Request<NameNodeUpdateAddrReq>,
    ) -> Result<Response<NameNodeUpdateAddrRes>, Status> {
        self.lock().unwrap().handle_update_addr(request)
    }
}

pub fn background_task(server: Arc<Mutex<NameNode>>) {
    loop {
        thread::sleep(Duration::seconds(5).to_std().unwrap());
        server.lock().unwrap().print_state();
    }
}
