use super::handlers::{
    send_heartbeat_handler, update_namenode_addr_handler, update_peers_addr_handler,
    update_peers_log_handler,
};
use super::LogLevel::SystemLog;
use crate::raft_dfs::{
    AppendEntriesReq, AppendEntriesRes, DataNodeConfig, DataNodeUpdateAddrReq,
    DataNodeUpdateAddrRes,
};
use chrono::offset::Local;
use chrono::Duration;
use futures::future;
use rand::Rng;
use tokio::time::timeout;
use tonic::{Request, Response, Status};

use super::{LogLevel, State};
// #[tonic::async_trait]
impl super::DataNode {
    pub fn handle_append_entries(
        &mut self,
        request: Request<AppendEntriesReq>,
    ) -> Result<Response<AppendEntriesRes>, Status> {
        let req = request.into_inner();
        let term = req.term;
        let id = req.id;
        let mut entries = req.entries;
        let index = req.index;
        self.dump_log(
            &format!(
                "receive append entries request\n-> term: {}, id: {}",
                term, id
            ),
            LogLevel::SystemLog,
        );
        self.raft_info.term = term;
        self.reset_timeout();
        let mut success = true;
        if (self.raft_info.file_logs.len() as i32) < index {
            self.dump_log(
                &format!("local file logs behind leader\n-> request for logs synchronized",),
                LogLevel::SystemLog,
            );
            success = false;
        } else {
            let mut logs = self.raft_info.file_logs[..index as usize].to_vec();
            logs.append(&mut entries);
            self.raft_info.file_logs = logs.clone();
            self.raft_info.log_index = logs.len() as i32;
        }
        let reply = AppendEntriesRes { success: success };

        Ok(Response::new(reply))
    }

    pub fn reset_timeout(&mut self) {
        let mut rng = rand::thread_rng();
        self.timeout = Local::now() + chrono::Duration::seconds(rng.gen_range(11..15));
    }

    pub fn check_timeout(&self) -> bool {
        if self.state == State::Leader {
            return false;
        }
        return Local::now() > self.timeout;
    }

    pub fn send_heartbeat(&mut self) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut drop_peers = Vec::new();
        let id = self.cfg.id.clone();
        let term = self.raft_info.term.clone();
        let logs = self.raft_info.file_logs.clone();
        let peers = self.peers.clone();

        rt.block_on(async move {
            for (peer_id, peer_addr) in peers.iter() {
                if peer_id.clone() == self.cfg.id {
                    continue;
                }
                let term = term;
                let id = id.clone();
                let index = self.raft_info.log_index;
                let entries = self.raft_info.file_logs[index as usize..].to_vec();
                let handler =
                    send_heartbeat_handler(peer_addr.clone(), term, id.clone(), entries, index);
                let timeout_res = timeout(Duration::seconds(2).to_std().unwrap(), handler).await;
                match timeout_res {
                    Ok(res) => match res {
                        Ok(append_entries_res) => {
                            let success = append_entries_res.into_inner().success;
                            if !success {
                                let _ = update_peers_log_handler(
                                    peer_addr.to_string(),
                                    id.clone(),
                                    term.clone(),
                                    logs.to_vec(),
                                )
                                .await;
                            }
                        }
                        Err(_) => {
                            drop_peers.push(DataNodeConfig {
                                id: peer_id.clone(),
                                addr: peer_addr.clone(),
                            });
                            self.drop_peer(peer_id, peer_addr)
                        }
                    },
                    Err(_) => {
                        drop_peers.push(DataNodeConfig {
                            id: peer_id.clone(),
                            addr: peer_addr.clone(),
                        });
                        self.drop_peer(peer_id, peer_addr);
                    }
                }
            }
            self.raft_info.log_index = self.raft_info.file_logs.len() as i32;
            if !drop_peers.is_empty() {
                let mut peers = Vec::<DataNodeConfig>::new();
                for (peer_id, peer_addr) in self.peers.iter() {
                    peers.push(DataNodeConfig {
                        id: peer_id.to_string(),
                        addr: peer_addr.to_string(),
                    });
                }
                let mut message = format!("cannot connect to node\n");
                for drop_peer in drop_peers.iter() {
                    message.push_str(&format!("-> id: {} addr: {}", drop_peer.id, drop_peer.addr));
                }
                let _ = update_namenode_addr_handler(message.clone(), peers.to_vec()).await;
                let mut handlers = Vec::new();
                for (peer_id, peer_addr) in self.peers.clone() {
                    if peer_id.clone() == self.cfg.id.clone() {
                        continue;
                    }
                    let handler =
                        update_peers_addr_handler(message.clone(), peer_addr, peers.to_vec());
                    handlers.push(handler);
                }
                let _ = future::join_all(handlers).await;
            }
        });
    }

    pub fn handle_update_addr(
        &mut self,
        request: Request<DataNodeUpdateAddrReq>,
    ) -> Result<Response<DataNodeUpdateAddrRes>, Status> {
        let req = request.into_inner();
        let peers = req.nodes;
        let message = req.message;
        self.dump_log(
            &format!("receive update addr request\n->  {}", message),
            LogLevel::SystemLog,
        );

        self.peers.clear();
        for peer in peers.iter() {
            self.peers.insert(peer.id.clone(), peer.addr.clone());
        }

        let reply = DataNodeUpdateAddrRes { success: true };

        Ok(Response::new(reply))
    }
    pub fn drop_peer(&mut self, peer_id: &String, peer_addr: &String) {
        self.dump_log(
            &format!(
                "cannot connect to node\n-> id: {}, addr: {}\n-> remove {} from local peers",
                peer_id, peer_addr, peer_id
            ),
            SystemLog,
        );
        self.peers.remove(peer_id);
    }
}
