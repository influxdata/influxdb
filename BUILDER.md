# Build influxdb3 with Jemalloc 16
## Multiarch build
```bash
docker buildx bake img-arm64`
docker import -i ./artifacts/influxdb3-multiarch.tar
```

## ARM64 only build
```bash
docker buildx bake img-arm64`
docker import -i ./artifacts/influxdb3-arm64.tar
```

## CentOS10 docker-ce on WSL2
To avoid segmentation error during build memory has to be preallocated:
.wslconfig
```
[wsl2]
neworkingMode=mirrored
dnsTunneling=true
firewall=true
memory=26GB
processors=13
swap=4GB
```





```bash
wsl
docker buildx create --name multibuilder --driver docker-container --buildkitd-config ./buildkitd.toml --use
```

## MacOS
brew install --cask docker