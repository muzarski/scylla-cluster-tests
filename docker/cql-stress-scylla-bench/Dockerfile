FROM rust:1.73 as builder

ARG BRANCH
ARG REPO

ENV BRANCH=${BRANCH:-master}
ENV REPO=${REPO:-https://github.com/scylladb/cql-stress.git}

RUN git clone ${REPO} -b ${BRANCH}

RUN cd cql-stress && cargo build --release --bin cql-stress-scylla-bench


FROM rust:1.73-slim as app

COPY --from=builder /cql-stress/target/release/cql-stress-scylla-bench /usr/local/bin/scylla-bench
