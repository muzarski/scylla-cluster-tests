### build from scylla repo
```
export CQL_STRESS_SB_DOCKER_IMAGE=scylladb/hydra-loaders:cql-stress-scylla-bench-$(date +'%Y%m%d')
docker build . -t ${CQL_STRESS_SB_DOCKER_IMAGE}
docker push ${CQL_STRESS_SB_DOCKER_IMAGE}
echo "${CQL_STRESS_SB_DOCKER_IMAGE}" > image
```
