// Proto compilation for Sui gRPC APIs
// Protos from: https://github.com/MystenLabs/sui-apis
// Commit: ee1220b276c407dd99a43394883b6d2d4957413f

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "sui-apis/proto";

    tonic_build::configure()
        .build_server(false) // Client only
        // Use extern_path to map google.rpc types to tonic's built-in types
        .extern_path(".google.rpc.Status", "::tonic::Status")
        .compile_protos(
            &[
                "sui-apis/proto/sui/rpc/v2/subscription_service.proto",
            ],
            &[proto_root],
        )?;

    // Re-run if protos change
    println!("cargo:rerun-if-changed={}", proto_root);

    Ok(())
}
