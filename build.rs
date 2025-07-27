fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("./src/rpc/proto")
        .compile_protos(&["./proto/exchange.proto"], &["./rpc/proto"])?;
    Ok(())
}
