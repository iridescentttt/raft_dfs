use super::LogLevel::FileLog;
use crate::raft_dfs::{
    DataNodeDeleteReq, DataNodeDeleteRes, DataNodeReadReq, DataNodeReadRes, DataNodeWriteReq,
    DataNodeWriteRes,
};
use std::fs::File;
use std::io::Write;
use std::str;
use std::sync::Mutex;
use std::{fs, sync::Arc};
use tonic::{Request, Response, Status};

impl super::DataNode {
    pub fn handle_read(
        &mut self,
        request: Request<DataNodeReadReq>,
    ) -> Result<Response<DataNodeReadRes>, Status> {
        let read_req = request.into_inner();
        let filename = read_req.filename;

        self.dump_log(&format!("read {:?}", filename), FileLog);

        let mut filepath = self.path.to_owned();
        filepath.push_str(&filename);
        let content = fs::read(&filepath).unwrap();
        let content = match str::from_utf8(&content) {
            Ok(v) => v,
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };

        let reply = DataNodeReadRes {
            content: format!("{}", content).into(),
        };

        Ok(Response::new(reply))
    }
    pub fn handle_write(
        &mut self,
        request: Request<DataNodeWriteReq>,
        file_handler: Arc<Mutex<File>>,
    ) -> (Result<Response<DataNodeWriteRes>, Status>, String, String) {
        let write_req = request.into_inner();
        let filename = write_req.filename;
        let content = write_req.content;

        self.dump_log(&format!("write {:?}", filename), FileLog);

        let mut filepath = self.path.to_owned();
        filepath.push_str(&filename);
        let _ = file_handler
            .lock()
            .unwrap()
            .write(content.clone().as_bytes());
        let reply = DataNodeWriteRes {
            message: format!("Sucessfully write!").into(),
        };
        (Ok(Response::new(reply)), filename, content)
    }

    pub fn handle_delete(
        &mut self,
        request: Request<DataNodeDeleteReq>,
    ) -> (Result<Response<DataNodeDeleteRes>, Status>, String) {
        let read_req = request.into_inner();
        let filename = read_req.filename;

        self.dump_log(&format!("delete {:?}", filename), FileLog);

        let mut filepath = self.path.to_owned();
        filepath.push_str(&filename);
        let delete_res = fs::remove_file(&filepath);

        let message = match delete_res {
            Ok(_) => "delete successfully",
            Err(_) => {
                self.dump_log(&format!("delete {:?} failed", filename), FileLog);
                "delete failed"
            }
        };

        let reply = DataNodeDeleteRes {
            message: format!("{}!", message).into(),
        };

        (Ok(Response::new(reply)), filename)
    }
}
