

WARN    := -Wall -Wstrict-prototypes -Wmissing-prototypes
INCLUDE := -isystem /lib/modules/`uname -r`/build/include

DEBUG:=-DDEBUG -g3
RELEASE:=-O3

CFLAGS  := ${WARN} ${INCLUDE}
CC      := gcc
OBJS    := ${patsubst %.c, %.o, ${wildcard *.c}}
LD			:= gcc
LDFLAGS	:= -o rrdb

.PHONY: multi
multi:
	$(MAKE) -j$(CPUCOUNT) default

debug: CFLAGS += $(DEBUG)
debug: all

release: CFLAGS += $(RELEASE)
release: all

default: release

all: ${OBJS}
	$(LD) $(LDFLAGS) *.o

.PHONY: clean

clean:
	rm -rf *.o

install: all
	cp rrdb /usr/bin/
