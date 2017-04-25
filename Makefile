WARN    := -W -Wall -Wstrict-prototypes -Wmissing-prototypes
INCLUDE := -isystem /lib/modules/`uname -r`/build/include

DEBUGFLAGS := -g
CFLAGS  := -O2 ${WARN} ${INCLUDE} ${DEBUGFLAGS}
CC      := gcc
OBJS    := ${patsubst %.c, %.o, ${wildcard *.c}}
LD			:= gcc
LDFLAGS	:= -O -o rrdb


all: ${OBJS}
	$(LD) $(LDFLAGS) *.o

.PHONY: clean

clean:
	rm -rf *.o

install: all
	cp rrdb /usr/bin/
