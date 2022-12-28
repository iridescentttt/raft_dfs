use chrono::{DateTime, Duration, Local};
use fs2::FileExt;
use rand::Rng;
use std::collections::HashMap;
use std::fs::File;
use std::str;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio;
use tonic::{Request, Response, Status};

use self::handlers::namenode_register_handler;
use crate::raft_dfs::data_node_service_server::DataNodeService;
use crate::raft_dfs::{
    AppendEntriesReq, AppendEntriesRes, DataNodeConfig, DataNodeDeleteReq, DataNodeDeleteRes,
    DataNodeReadReq, DataNodeReadRes, DataNodeUpdateAddrReq, DataNodeUpdateAddrRes,
    DataNodeWriteReq, DataNodeWriteRes, RequestVoteReq, RequestVoteRes, UpdateLogReq, UpdateLogRes,
};
use crate::utils::{datanode_delete_handler, datanode_write_handler};

pub mod file_service;
pub mod handlers;
pub mod heartbeat;
pub mod vote;

#[derive(Debug, PartialEq)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}
#[derive(Debug, PartialEq)]
pub enum LogLevel {
    FileLog,
    SystemLog,
}

#[derive(Debug, Default, Clone)]
pub struct RaftInfo {
    pub term: i32,
    pub voted_for: Option<DataNodeConfig>,
    pub file_logs: Vec<String>,
    pub log_index: i32,
}

#[derive()]
pub struct DataNode {
    pub cfg: DataNodeConfig,
    pub path: String,
    pub state: State,
    pub raft_info: RaftInfo,
    pub timeout: DateTime<Local>,
    pub peers: HashMap<String, String>,
    pub sys_logs: Vec<String>,
}

impl DataNode {
    pub fn builder(id: String, path: String, addr: String) -> DataNode {
        DataNode {
            cfg: DataNodeConfig { id: id, addr: addr },
            path: path,
            state: State::Follower,
            raft_info: RaftInfo::default(),
            timeout: chrono::offset::Local::now() + Duration::seconds(5),
            peers: HashMap::new(),
            sys_logs: Vec::new(),
        }
    }

    fn build_log(&self, message: &str) -> String {
        String::from(format!(
            "[{}] {} | term {} | {:?} \n-> {}",
            chrono::offset::Local::now()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            self.cfg.id.to_string(),
            self.raft_info.term,
            self.state,
            message
        ))
    }

    fn dump_log(&mut self, message: &str, level: LogLevel) -> () {
        let log = self.build_log(message);
        if level == LogLevel::FileLog {
            self.raft_info.log_index += 1;
            self.raft_info.file_logs.push(log.clone());
        }
        println!("{}\n", log);
    }
}

#[tonic::async_trait]
impl DataNodeService for Mutex<DataNode> {
    async fn read(
        &self,
        request: Request<DataNodeReadReq>,
    ) -> Result<Response<DataNodeReadRes>, Status> {
        self.lock().unwrap().handle_read(request)
    }
    async fn write(
        &self,
        request: Request<DataNodeWriteReq>,
    ) -> Result<Response<DataNodeWriteRes>, Status> {
        let lock_req = request.get_ref().clone();
        let mut filepath = self.lock().unwrap().path.to_owned();
        let lock_filename = lock_req.filename;
        filepath.push_str(&lock_filename);
        let file_handler = File::create(filepath)?;
        file_handler.lock_exclusive()?;
        let file_arc = Arc::new(Mutex::new(file_handler));
        let (res, filename, content) = self
            .lock()
            .unwrap()
            .handle_write(request, Arc::clone(&file_arc));
        let is_leader = self.lock().unwrap().state == State::Leader;
        if is_leader {
            let peers = self.lock().unwrap().peers.clone();
            let id = self.lock().unwrap().cfg.id.clone();
            for (peer_id, peer_addr) in peers.iter() {
                if peer_id.clone() == id.clone() {
                    continue;
                }
                let _ =
                    datanode_write_handler(filename.clone(), content.clone(), peer_addr.clone())
                        .await;
            }
        }
        let unlock_arc = Arc::clone(&file_arc);
        unlock_arc.lock().unwrap().unlock()?;
        res
    }

    async fn delete(
        &self,
        request: Request<DataNodeDeleteReq>,
    ) -> Result<Response<DataNodeDeleteRes>, Status> {
        let lock_req = request.get_ref().clone();
        let mut filepath = self.lock().unwrap().path.to_owned();
        let lock_filename = lock_req.filename;
        filepath.push_str(&lock_filename);
        let file = File::create(filepath)?;
        file.lock_exclusive()?;
        let (res, filename) = self.lock().unwrap().handle_delete(request);
        let is_leader = self.lock().unwrap().state == State::Leader;
        if is_leader {
            let peers = self.lock().unwrap().peers.clone();
            let id = self.lock().unwrap().cfg.id.clone();
            for (peer_id, peer_addr) in peers.iter() {
                if peer_id.clone() == id.clone() {
                    continue;
                }
                let _ = datanode_delete_handler(filename.clone(), peer_addr.clone()).await;
            }
        }
        file.unlock()?;
        res
    }
    async fn append_entries(
        &self,
        request: Request<AppendEntriesReq>,
    ) -> Result<Response<AppendEntriesRes>, Status> {
        // self.trigger.0.trigger();
        self.lock().unwrap().handle_append_entries(request)
    }
    async fn request_vote(
        &self,
        request: Request<RequestVoteReq>,
    ) -> Result<Response<RequestVoteRes>, Status> {
        self.lock().unwrap().handle_request_vote(request)
    }
    async fn update_log(
        &self,
        request: Request<UpdateLogReq>,
    ) -> Result<Response<UpdateLogRes>, Status> {
        self.lock().unwrap().handle_update_log(request)
    }
    async fn update_addr(
        &self,
        request: Request<DataNodeUpdateAddrReq>,
    ) -> Result<Response<DataNodeUpdateAddrRes>, Status> {
        self.lock().unwrap().handle_update_addr(request)
    }
}

fn handle_timeout(server: Arc<Mutex<DataNode>>) {
    let timeout = server.lock().unwrap().check_timeout();
    if timeout {
        server
            .lock()
            .unwrap()
            .dump_log("timeout. start election", LogLevel::SystemLog);
        server.lock().unwrap().election();
    }
}

fn handle_heartbeat(server: Arc<Mutex<DataNode>>) {
    let is_leader = server.lock().unwrap().state == State::Leader;
    if is_leader {
        server
            .lock()
            .unwrap()
            .dump_log("send heartbeat", LogLevel::SystemLog);
        server.lock().unwrap().reset_timeout();
        server.lock().unwrap().send_heartbeat();
    }
}

pub fn background_task(server: Arc<Mutex<DataNode>>) {
    loop {
        let mut rng = rand::thread_rng();
        thread::sleep(Duration::seconds(rng.gen_range(5..10)).to_std().unwrap());
        handle_timeout(Arc::clone(&server));
        handle_heartbeat(Arc::clone(&server));
    }
}

pub fn register_task(server: Arc<Mutex<DataNode>>) {
    let id = server.lock().unwrap().cfg.id.clone();
    let addr = server.lock().unwrap().cfg.addr.clone();
    tokio::spawn(async move {
        let handler = async move { namenode_register_handler(id, addr).await };
        let res = handler.await;
        match res {
            Ok(res) => {
                let register_res = res.into_inner();
                let peers = register_res.nodes;
                let leader_id = register_res.leader_id;
                server.lock().unwrap().dump_log(
                    &format!("register successfully\n->{:?}", peers),
                    LogLevel::SystemLog,
                );
                for peer_config in peers.iter() {
                    let peer_id = peer_config.id.clone();
                    let peer_addr = peer_config.addr.clone();
                    server
                        .lock()
                        .unwrap()
                        .peers
                        .insert(peer_id.to_string(), peer_addr.to_string());
                }

                if leader_id != "None" {
                    server.lock().unwrap().reset_timeout();
                }
            }
            Err(err) => println!("Error: {:?}", err),
        }
    });
}
