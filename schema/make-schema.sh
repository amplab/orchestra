# for rust
protoc --rust_out ../src/ comm.proto
protoc --rust_out ../src/ types.proto

# for python
protoc --python_out ../lib/python comm.proto
protoc --python_out ../lib/python types.proto
