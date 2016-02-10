# for rust
protoc --rust_out ../src/ comm.proto
protoc --rust_out ../src/ types.proto

# for cprotobuf
protoc --cprotobuf_out ../lib/orchpy/orchpy comm.proto types.proto
