use futures::{future, Future, Sink};
use crate::delorean::{
    server,
    CreateBucketRequest,
    CreateBucketResponse,
    GetBucketsResponse,
    DeleteBucketRequest,
    DeleteBucketResponse,
    Bucket,
    Organization,
};
use std::sync::{Arc, Mutex};
use std::fmt::Error;
use tower_grpc::{Request, Response};
use tower_hyper::server::{Http, Server};
use tokio::net::TcpListener;
use tower_util::Ready;

pub mod delorean {
    include!(concat!(env!("OUT_DIR"), "/delorean.rs"));
}

//#[derive(Debug, Clone)]
//struct Delorean {
//    buckets: Arc<Vec<Bucket>>,
//}
//
//impl delorean::server::Delorean for Delorean {
//    type CreateBucketFuture = future::FutureResult<Response<CreateBucketResponse>, tower_grpc::Status>;
//    fn create_bucket(&mut self, request: Request<CreateBucketRequest>) -> Self::CreateBucketFuture {
//        println!("CreateBucket: {:?}", request);
//        let response = Response::new(CreateBucketResponse{});
//        future::ok(response)
//    }
//
//    type DeleteBucketFuture = future::FutureResult<Response<DeleteBucketResponse>, tower_grpc::Status>;
//    //futures::future::result_::FutureResult<tower_grpc::response::Response<delorean::DeleteBucketResponse>, tower_grpc::status::Status>
//    fn delete_bucket(&mut self, request: Request<DeleteBucketRequest>) -> Self::DeleteBucketFuture {
//        println!("DeleteBucket: {:?}", request);
//        future::ok(Response::new(DeleteBucketResponse{}))
//    }
//
//    type GetBucketsFuture = future::FutureResult<Response<GetBucketsResponse>, tower_grpc::Status>;
//    fn get_buckets(&mut self, request: Request<Organization>) -> Self::GetBucketsFuture {
//        println!("GetBuckets: {:?}", request);
//        future::ok(Response::new(GetBucketsResponse{buckets: self.buckets.clone().to_vec()}))
//    }
//}

fn main() {
    println!("Hello, world!");

//    let handler = Delorean {
//        buckets: Arc::new(vec![]),
//    };
//
//    let new_service = server::DeloreanDbServer::new(handler);
//    let mut server = Server::new(new_service);
//    let http = Http::new().http2_only(true).clone();
//
//    let addr = "127.0.0.1:10000".parse().unwrap();
//    let bind = TcpListener::bind(&addr).expect("bind");
//
//    println!("listening on {:?}", addr);
//
//    let serve = bind
//        .incoming()
//        .for_each(move |sock| {
//            if let Err(e) = sock.set_nodelay(true) {
//                return Err(e);
//            }
//
//            let serve = server.serve_with(sock, http.clone());
//            tokio::spawn(serve.map_err(|e| println!("h2 error: {:?}", e)));
//
//            Ok(())
//        })
//        .map_err(|e| eprintln!("accept error: {}", e));
//
//    tokio::run(serve);
}
