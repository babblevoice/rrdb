
# docker build . -t <your username>/projectrtp

FROM alpine:latest as builder

WORKDIR /usr/src/rrdb
COPY . .

# build rrdb
RUN apk add --no-cache alpine-sdk; \
    make clean && make && make install


FROM alpine:latest as app

#stunnel - removed
RUN apk add --no-cache ucspi-tcp6 

COPY --from=builder /usr/bin/rrdb /usr/bin/rrdb

EXPOSE 13900
WORKDIR /var/rrdb
CMD [ "/usr/bin/tcpserver", "0", "13900", "/usr/bin/rrdb", "--command=-", "--dir=/var/rrdb" ]

