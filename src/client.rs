use std::{env, fs, path::Path};
mod raft_dfs;

mod utils;
use std::str;
use utils::{delete, read, write};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let method: String;
    if args.len() <= 1 {
        method = String::from("");
    } else {
        method = String::from(&args[1]);
    }
    match method.as_str() {
        "read" => {
            fs::create_dir_all("cache/")?;
            let filename = String::from(&args[2]);
            let cache_found = Path::new(&format!("cache/{}", filename)).exists();
            let read_res: String;
            if cache_found {
                let content = fs::read(&format!("cache/{}", filename)).unwrap();
                read_res = match str::from_utf8(&content) {
                    Ok(v) => v.into(),
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                };
            } else {
                read_res = read(filename.clone().into()).await.unwrap();
                let _ = fs::write(&format!("cache/{}", filename), &read_res);
            }
            println!("{}", read_res);
        }
        "write" => {
            if args.len() <= 3 {
                println!(
                    "wrong method! try\n-> read filename\n-> write filename content \n-> delete filename\n"
                );
            } else {
                let filename = String::from(&args[2]);
                let content = String::from(&args[3]);
                let _ = fs::write(&format!("cache/{}", filename),&content);
                let _ = write(filename.into(), content.into()).await.unwrap();
            }
        }
        "delete" => {
            if args.len() <= 2 {
                println!(
                    "wrong method! try\n-> read filename\n-> write filename content \n-> delete filename\n"
                );
            } else {
                let filename = String::from(&args[2]);
                let cache_found = Path::new(&format!("cache/{}", filename)).exists();
                if cache_found {
                    let _ = fs::remove_file(&format!("cache/{}", filename)).unwrap();
                }
                let _ = delete(filename.into()).await.unwrap();
            }
        }
        _ => {
            println!(
                "wrong method! try\n-> read filename\n-> write filename content \n-> delete filename\n"
            );
        }
    }

    Ok(())
}
