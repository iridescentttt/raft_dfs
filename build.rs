fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/raft_dfs.proto").unwrap();
    Ok(())
}
