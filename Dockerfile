
# docker build . -t <your username>/projectrtp

FROM alpine:latest as builder

WORKDIR /usr/src/rrdb
COPY . .

# build rrdb
RUN apk add --no-cache alpine-sdk; \
    make clean && make


FROM alpine:latest as app


COPY --from=builder [ "/usr/src/rrdb", "/usr/src/rrdb" ]

EXPOSE 13900
WORKDIR /var/rrdb
CMD [ "/usr/bin/nc", "-lk", "-p", "13900", "-e", "/usr/src/rrdb/rrdb", "--command=-", "--dir=/var/rrdb" ]

