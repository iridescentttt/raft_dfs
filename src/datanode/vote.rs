use super::handlers::{
    request_vote_handler, update_namenode_addr_handler, update_namenode_leader_handler,
    update_peers_log_handler,
};
use super::LogLevel::SystemLog;
use super::{LogLevel, State};
use crate::raft_dfs::{DataNodeConfig, RequestVoteReq, RequestVoteRes, UpdateLogReq, UpdateLogRes};
use chrono::Duration;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::time::timeout;
use tonic::{Request, Response, Status};
// #[tonic::async_trait]
impl super::DataNode {
    pub fn handle_update_log(
        &mut self,
        request: Request<UpdateLogReq>,
    ) -> Result<Response<UpdateLogRes>, Status> {
        let req = request.into_inner();
        let id = req.id;
        let term = req.term;
        let logs = req.logs;
        self.dump_log(
            &format!("receive update log request\n->term: {}, id: {}", term, id),
            LogLevel::SystemLog,
        );
        self.raft_info.file_logs = logs;
        self.reset_timeout();

        let reply = UpdateLogRes { success: true };
        Ok(Response::new(reply))
    }

    pub fn handle_request_vote(
        &mut self,
        request: Request<RequestVoteReq>,
    ) -> Result<Response<RequestVoteRes>, Status> {
        let vote_req = request.into_inner();
        let term = vote_req.term;
        let candidate_id = String::from(vote_req.candidate_id);
        let message = match self.raft_info.voted_for {
            Some(_) => {
                // larger term
                if term > self.raft_info.term {
                    // update current vote for
                    self.raft_info.voted_for = Some(DataNodeConfig {
                        id: candidate_id.clone(),
                        addr: self.peers.get(&candidate_id).unwrap().to_string(),
                    });
                    self.raft_info.term = term;
                    self.dump_log(
                        &format!(
                            "receive vote request \n-> term: {}, id: {}\n-> accept",
                            term, candidate_id
                        ),
                        SystemLog,
                    );
                    RequestVoteRes {
                        term: self.raft_info.term,
                        vote_granted: true,
                    }
                } else {
                    self.dump_log(
                    &format!(
                        "receive vote request \n-> term: {}, id: {}\n-> self.raft_info.voted_for is not none\n-> refuse",
                        term, candidate_id
                    ),
                    SystemLog,
                );
                    RequestVoteRes {
                        term: self.raft_info.term,
                        vote_granted: false,
                    }
                }
            }
            None => {
                if term >= self.raft_info.term {
                    // update current vote for
                    self.raft_info.voted_for = Some(DataNodeConfig {
                        id: candidate_id.clone(),
                        addr: self.peers.get(&candidate_id).unwrap().to_string(),
                    });
                    self.raft_info.term = term;
                    self.dump_log(
                        &format!(
                            "receive vote request \n-> term: {}, id: {}\n-> accept",
                            term, candidate_id
                        ),
                        SystemLog,
                    );
                    RequestVoteRes {
                        term: self.raft_info.term,
                        vote_granted: true,
                    }
                } else {
                    self.dump_log(
                        &format!(
                            "receive vote request \n-> term: {}, id: {}\n-> current term is larger\n -> refuse",
                            term, candidate_id
                        ),
                        SystemLog,
                    );
                    RequestVoteRes {
                        term: self.raft_info.term,
                        vote_granted: false,
                    }
                }
            }
        };

        Ok(Response::new(message))
    }

    pub fn election(&mut self) {
        self.raft_info.term += 1;
        self.state = State::Candidate;
        self.raft_info.voted_for = Some(DataNodeConfig::from(self.cfg.clone()));
        let votes = Mutex::new(1);
        let id = self.cfg.id.clone();
        let term = self.raft_info.term.clone();
        let logs = self.raft_info.file_logs.clone();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut id_map = HashMap::new();
        for (index, (peer_id, _)) in self.peers.clone().iter().enumerate() {
            if peer_id.clone() == id.clone() {
                continue;
            }
            id_map.insert(index.clone(), peer_id.clone());
        }
        rt.block_on(async {
            for (peer_id, peer_addr) in self.peers.clone().iter() {
                if peer_id.clone() == id.clone() {
                    continue;
                }
                println!("send vote request to peer {}", peer_id);
                let vote_id = String::from(&self.cfg.id);
                let vote_term = self.raft_info.term.clone();
                let handler = request_vote_handler(peer_addr.clone(), vote_term, vote_id);
                let timeout_res = timeout(Duration::seconds(2).to_std().unwrap(), handler).await;
                match timeout_res {
                    Ok(res) => match res {
                        Ok(res) => {
                            let vote_res = res.into_inner();
                            let granted = vote_res.vote_granted;
                            if granted {
                                let mut guarded_votes = votes.lock().unwrap();
                                *guarded_votes += 1;
                            };
                        }
                        Err(_) => self.drop_peer(peer_id, peer_addr),
                    },
                    Err(_) => {
                        self.drop_peer(peer_id, peer_addr);
                    }
                }
            }
        });
        rt.block_on(async {
            self.dump_log(
                &format!("receive vote {}", votes.lock().unwrap().clone()),
                LogLevel::SystemLog,
            );
            let peers_num = self.peers.len();
            if votes.lock().unwrap().clone() * 2 > peers_num {
                self.dump_log("become leader", SystemLog);
                self.state = State::Leader;
                let _ = update_namenode_leader_handler(id.clone(), term.clone()).await;
                self.dump_log("update leader in namenode", SystemLog);
            }
        });
        // update log
        rt.block_on(async {
            if self.state == State::Leader {
                let mut nodes = Vec::<DataNodeConfig>::new();
                self.dump_log("sync logs in peers", SystemLog);
                for (peer_id, peer_addr) in self.peers.clone() {
                    nodes.push(DataNodeConfig {
                        id: peer_id.clone(),
                        addr: peer_addr.clone(),
                    });
                    if peer_id.clone() == self.cfg.id {
                        continue;
                    }
                    let _ = update_peers_log_handler(
                        peer_addr,
                        id.clone(),
                        term.clone(),
                        logs.to_vec(),
                    )
                    .await;
                }
                let message = format!("sync leader addr");
                let _ = update_namenode_addr_handler(message, nodes).await;
                self.dump_log("sync addr in namenode", SystemLog);
            }
        });
    }
}
