use tonic::{Request, Response, Status};

use super::LogLevel::FileLog;
use crate::raft_dfs::{
    NameNodeDeleteReq, NameNodeDeleteRes, NameNodeReadReq, NameNodeReadRes, NameNodeWriteReq,
    NameNodeWriteRes,DataNodeConfig
};

impl super::NameNode {
    pub fn handle_read(
        &mut self,
        request: Request<NameNodeReadReq>,
    ) -> Result<Response<NameNodeReadRes>, Status> {
        let read_req = request.into_inner();
        let filename = read_req.filename;

        self.dump_log(&format!("read {}", filename), FileLog);

        let leader_cfg = match &self.leader_cfg {
            Some(x) => x.clone(),
            None => DataNodeConfig::default(),
        };
        let reply = NameNodeReadRes {
            id: leader_cfg.id.clone(),
            addr: leader_cfg.addr.clone(),
        };
        Ok(Response::new(reply))
    }
    pub fn handle_write(
        &mut self,
        request: Request<NameNodeWriteReq>,
    ) -> Result<Response<NameNodeWriteRes>, Status> {
        let read_req = request.into_inner();
        let filename = read_req.filename;

        self.dump_log(&format!("write {}", filename), FileLog);

        let leader_cfg = match &self.leader_cfg {
            Some(x) => x.clone(),
            None => DataNodeConfig::default(),
        };
        let reply = NameNodeWriteRes {
            id: leader_cfg.id.clone(),
            addr: leader_cfg.addr.clone(),
        };
        Ok(Response::new(reply))
    }

    pub fn handle_delete(
        &mut self,
        request: Request<NameNodeDeleteReq>,
    ) -> Result<Response<NameNodeDeleteRes>, Status> {
        let read_req = request.into_inner();
        let filename = read_req.filename;

        self.dump_log(&format!("delete {}", filename), FileLog);

        let leader_cfg = match &self.leader_cfg {
            Some(x) => x.clone(),
            None => DataNodeConfig::default(),
        };
        let reply = NameNodeDeleteRes {
            id: leader_cfg.id.clone(),
            addr: leader_cfg.addr.clone(),
        };
        Ok(Response::new(reply))
    }
}
