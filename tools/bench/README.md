# Chorus benchmark tool
See [config](pkg/config/conf.go). 

- The tool writes objects of **configured size** to **configured bucket** to ChorusProxy with **configured concurrency**.
- Write benchmarks logged to `CSV` file along with chorus meta.
- Every `N` writes the tool performs `LIST` and `GET` object against ChorusProxy and directly to main storage.
- Proxy and Main `LIST` and `GET` results also logged to a separate `CSV` files along with Chorus meta.

This allows to see how ChorusProxy behaves dependent on number of objects in bucket in comparison to Main storage.

Other features:
- The tool stores stated in local KV store on disk `chorus-bench.db` and will catch up from the same place in case of restart.



### Build binary for linux
```shell
GOOS=linux GOARCH=amd64 go build -o bench-linux-64 .
```

### Run in background
```shell
nohup bench-linux-64 > chorus_bench.log &
```
Will start benchmark in background and redirect logs into `chorus_bench.log` file.
Benchmarks results will appear in `CSV` files `bench_*_<obj_size>_P<concurrency>_<timestamp>.csv` in current directory.

