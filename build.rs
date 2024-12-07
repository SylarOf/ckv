fn main() {
    prost_build::Config::new().out_dir("src/pb").
    compile_protos(&["src/pb/pb.proto"], &["src/pb"])
        .expect("Failed to compile Protobuf files");
}
