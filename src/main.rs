use std::env;
use std::process;
use bytes::Bytes;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, Client};
use aws_sdk_s3::config::Credentials;
use std::time::SystemTime;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let arg1: &String = &args[1];
    let file_path: String = arg1.to_owned();

    check_s3_uri(&file_path);
    println!("Starting to work on file: {}", &file_path);

    let (bucket, key) = split_s3_uri_into_bucket_and_key(file_path);
    let data = pull_file_from_s3_to_bytes(&bucket, &key).await;
    println!("File pulled from S3:");
    let csv_data: String = convert_bytes_to_string(data);
    println!("File converted to string:");
    let tab_data: String = convert_fixed_width_to_tab(csv_data);
    println!("File converted to tab");
    let new_key = format!("{}{}", key, ".tab");
    println!("New key: {}", &new_key);
    push_bytes_to_s3(&bucket, &new_key, Bytes::from(tab_data)).await;
}

fn check_s3_uri(uri: &String) -> bool {
    if uri.starts_with("s3://") {
        return true;
    }
    println!("Invalid S3 URI: {}", uri);
    process::exit(1);
}

fn split_s3_uri_into_bucket_and_key(mut file_path: String) -> (String, String) {
    file_path.replace_range(0..5, "");
    let parts:  Vec<&str> = file_path.split("/").collect();
    let bucket: String = parts[0].to_owned();
    let key: String = parts[1].to_owned();
    (bucket, key)
}

async fn build_config_and_client() -> Client {
    let aws_access_key_id = env::var_os("aws_access_key_id").expect("not found").into_string().expect("failed");
    let aws_secret_access_key = env::var_os("aws_secret_access_key").expect("not found").into_string().expect("failed");
    let keys = Credentials::new(
        aws_access_key_id,
        aws_secret_access_key,
        None,
        None,
        "dummy",
    );
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env()
        .region(region_provider)
        .credentials_provider(keys)
        .load()
        .await;
    let client: Client = Client::new(&config);
    return client;
}

async fn pull_file_from_s3_to_bytes(bucket: &String, key: &String) -> Bytes {
    let s3_client = build_config_and_client().await;
    let data = s3_client
                                .get_object()
                                .bucket(bucket)
                                .key(key)
                                .send()
                                .await.unwrap().body
                                .collect().await.unwrap().into_bytes();
    return data;
}

async fn push_bytes_to_s3(bucket: &String, key: &String, data: Bytes) {
    let s3_client = build_config_and_client().await;
    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(data.into())
        .send()
        .await
        .unwrap();
}

fn convert_bytes_to_string(data: Bytes) -> String {
    let data_string: String = String::from_utf8(data.to_vec()).unwrap();
    return data_string;
}

fn convert_fixed_width_to_tab(data: String) -> String {
    let mut tab_converted: String = String::new();
    let result_vector: Vec<&str> = data.split("\n").collect::<Vec<&str>>();
    for line in result_vector.iter() {
        if line.is_empty() {
            continue;
        }
        let date: &&str = &line[0..14].trim();
        let serial_number: &&str = &line[15..35].trim();
        let model: &&str = &line[36..78].trim();
        let capacity_bytes: &&str = &line[79..97].trim();
        let failure: &&str = &line[98..108].trim();
        let tab_line: String = format!( "{}\t{}\t{}\t{}\t{}\n", date, serial_number, model, capacity_bytes, failure);
        tab_converted.push_str(&tab_line);
        }
    return tab_converted;
}