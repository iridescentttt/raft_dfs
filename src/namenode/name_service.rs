use std::collections::HashMap;

use tonic::{Request, Response, Status};

use super::{ LogLevel::SystemLog};
use crate::raft_dfs::{
    DataNodeConfig, NameNodeUpdateAddrReq, NameNodeUpdateAddrRes, RegisterReq, RegisterRes,
    UpdateLeaderReq, UpdateLeaderRes,
};

impl super::NameNode {
    pub fn handle_register(
        &mut self,
        request: Request<RegisterReq>,
    ) -> (
        Result<Response<RegisterRes>, Status>,
        DataNodeConfig,
        HashMap<String, String>,
        Vec<DataNodeConfig>,
    ) {
        let register_req = request.into_inner();
        let id = register_req.id;
        let addr = register_req.addr;

        self.dump_log(
            &format!("datanode register -> id: {}, addr: {}, ", id, addr),
            SystemLog,
        );

        self.nodes.insert(id.clone(), addr.clone());

        let register_node = DataNodeConfig {
            id: id.clone(),
            addr: addr.clone(),
        };
        let nodes_addr = self.nodes.clone();
        let mut nodes = Vec::<DataNodeConfig>::new();
        for (node_id, node_addr) in nodes_addr.iter() {
            nodes.push(DataNodeConfig {
                id: node_id.clone(),
                addr: node_addr.clone(),
            });
        }

        let leader_cfg = match &self.leader_cfg {
            Some(cfg) => cfg.clone(),
            None => DataNodeConfig::default(),
        };
        let reply = RegisterRes {
            leader_id: leader_cfg.id,
            leader_addr: leader_cfg.addr,
            nodes: nodes.to_vec(),
        };
        (Ok(Response::new(reply)), register_node, nodes_addr, nodes)
    }

    pub fn handle_update_leader(
        &mut self,
        request: Request<UpdateLeaderReq>,
    ) -> Result<Response<UpdateLeaderRes>, Status> {
        let update_leader_req = request.into_inner();
        let id = update_leader_req.id;
        let node_addr = self.nodes.clone();
        let addr = node_addr.get(&id).unwrap();
        let term = update_leader_req.term;

        self.dump_log(
            &format!("update leader ->\nid: {}, addr: {}", id, addr),
            SystemLog,
        );
        self.leader_cfg = Some(DataNodeConfig {
            id: id,
            addr: addr.to_string(),
        });
        self.term = term;

        let reply = UpdateLeaderRes { success: true };

        Ok(Response::new(reply))
    }
    pub fn handle_update_addr(
        &mut self,
        request: Request<NameNodeUpdateAddrReq>,
    ) -> Result<Response<NameNodeUpdateAddrRes>, Status> {
        let req = request.into_inner();
        let nodes = req.nodes;
        let message = req.message;
        self.nodes.clear();
        for node in nodes.into_iter() {
            self.nodes.insert(node.id, node.addr);
        }

        self.dump_log(&format!("update nodes addr\n->{}", message), SystemLog);

        let reply = NameNodeUpdateAddrRes { success: true };

        Ok(Response::new(reply))
    }
    pub fn print_state(&mut self) {
        let node_num = self.nodes.len();
        let term = self.term;
        let cfg= self.leader_cfg.clone().unwrap();
        self.dump_log(
            &format!(
                "Total Nodes: {} | Term: {} | Leader ID {} | Leader Addr {}",
                node_num, term, cfg.id, cfg.addr
            ),
            SystemLog,
        );
    }
}
