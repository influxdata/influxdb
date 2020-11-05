use std::env;

fn main() {
    if let Ok(file) = dotenv::dotenv() {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    if env::var("GCS_BUCKET_NAME").is_ok() && env::var("SERVICE_ACCOUNT").is_ok() {
        println!("cargo:rustc-cfg=test_gcs");
    }

    if env::var("AWS_DEFAULT_REGION").is_ok()
        && env::var("AWS_S3_BUCKET_NAME").is_ok()
        && env::var("AWS_ACCESS_KEY_ID").is_ok()
        && env::var("AWS_SECRET_ACCESS_KEY").is_ok()
    {
        println!("cargo:rustc-cfg=test_aws");
    }
}
