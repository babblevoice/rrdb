
# docker build . -t <your username>/projectrtp

FROM alpine:latest as builder

WORKDIR /usr/src/rrdb
COPY . .

# build rrdb
RUN apk add --no-cache alpine-sdk; \
    make clean && make && make install


FROM alpine:latest as app

RUN apk add --no-cache stunnel

COPY --from=builder /usr/bin/rrdb /usr/bin/rrdb

EXPOSE 13900
WORKDIR /var/rrdb
CMD [ "/usr/bin/nc", "-lk", "-p", "13900", "-e", "/usr/bin/rrdb", "--command=-", "--dir=/var/rrdb" ]

