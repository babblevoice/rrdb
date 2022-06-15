# RRDB

Round Robin Database designed for speed and efficiency. Low memory, small disk footprint. High speed for both reading and writing.

## Introduction

Data manipulation - store and retreive round robin data. Maintain transformations (xforms) i.e. average, sum, count etc.
Can be run as command or as pipe mode (for inet server).

For example, imagine we wish to keep the average phone call queue duration in Asterisk.

We would initialize our database file as follows:
rrdb --command=create --dir=/data/rrd --filename=test.rrdb --setcount=1 --samplecount=500 --xform=RRDBCOUNT:ONEDAY:RRDBMEAN:ONEDAY:0

This creates a single db file, for one set of data (in this example for call queue duration). We could create it to include other params we want are interested in also.

The xform param creates extra columns which store the values - count for the period and average value for that period and column.

When a phone call is answered, Asterisk would run the command:
rrdb --command=update --dir=/data/rrd --filename=test.rrdb --value=12

Then, probably in a web app, to get the data to render a graph for example:
rrdb --command=fetch --dir=/data/rrd --filename=test.rrdb --xform=0

Which would return:
1494604800:1

or:
rrdb --command=fetch --dir=/data/rrd --filename=test.rrdb
1494606092.19696:12


# Commands
## pipe, run the program and wait for input to describe the task with the format

In pipe mode it waits for user input as described in the following commands. This is so that it can be run as a server.

## command filename params (specific to command)
## create

Create a new data file, the details about how to store are provided. Set count (how many data sets) sample count(how many historical points
to keep). xforms - storage of averages sums etc.

The xforms are:
RRDBMAX
RRDBMIN
RRDBCOUNT
RRDBMEAN
RRDBSUM

All xforms will maintain a new data set for each set in the file, apart from RRDBCOUNT, which will simply maintain 1 data set for count (i.e. quantity of events).
and the time spans are:
FIVEMINUTE
ONEHOUR
SIXHOUR
TWELVEHOUR
ONEDAY

### Examples

Pipe mode:

create test.rrdb 0 500 RRDBCOUNT:ONEDAY
create test.rrdb 1 500 RRDBCOUNT:ONEDAY:RRDBCOUNT:FIVEMINUTE:RRDBSUM:FIVEMINUTE:0

Command line:

rrdb --command=create --dir=/data/rrd --filename=nick.rrdb --setcount=0 --samplecount=500 --xform=RRDBCOUNT:ONEDAY

## update

Updates the database with some data.

### Examples

Pipe mode:
update test.rrdb 0:1

## modify

This shouldn't be used for general use - but we have had cases where corrupt data can creep in (from the calling application) and the user wants to get rid of it as it can throw renderin of graphs out.

rrdb --command=modify --dir=/data/rrd --filename=babblegroup_1:queue_abandoned.rrdb --values=1624024800:0 --xform=0

The value to be modified is indexed by either raw or a particular xform. the by time followed by the new value.

### Examples

Pipe mode:

update test.rrdb 0
update test.rrdb 4643

Command line:

rrdb --command=update --dir=/data/rrd --filename=nick.rrdb --value=12

## fetch
To fetch the table of data within the sets.

### Examples

Pipe mode:
fetch test.rrdb

To fetch a single column table for our xforms (in this case the RDBCOUNT:ONEDAY)
fetch test.rrdb 0

Command line:
rrdb --command=fetch --dir=/data/rrd --filename=nick.rrdb
rrdb --command=fetch --dir=/data/rrd --filename=nick.rrdb --xform=0

The first will return all of the raw data in columns.
The second with the param --xform=0 will return xform data set 0 (each set may have different times against rows).

## info

Get information regarding the RRDB file.

### Examples

info test.rrdb

# V2 Touch

Version 2 introduced a new method - touch. The two types of file cannot be mixed. V2 Touch addresses named columns (paths) which maybe 'touched' (i.e. an event has occurred with reference to the column).

Records a count against a path (the named column). The path is comma delimitated, so that it will record a touch against the whole touchpath and also each item
which is comma delimitated. Unlike version 1, V2 Touch does not need a separate create and update there is only a touch command (well fetch as well).

truncate: the number of sets after which it will start to re-use older non used sets
touchpath: the string we are counting against.

## Examples

Command line:
rrdb --command=touch --dir=/data/rrd --filename=nick.rrdb --touchpath=test
rrdb --command=touch --dir=/data/rrd --filename=nick.rrdb --touchpath=tech,support --samplecount=2000 --period=ONEHOUR,ONEDAY --setcount=50

And fetch the data of a specific path and period:
rrdb --dir=./ --filename=nick.rrdb --command=fetch --touchpath=emisbookingsuccessful --period=ONEDAY

In pipe mode, these are the equivalent commands

touch test.rrdb 50 2000 tech,support ONEHOUR,ONEDAY
touch <filename> <setcount> <samplecount> <path> <period>
(setcount could also be described as max setcount)

# Building

On Linux, with build tools installed, go into src folder
make
make install

# Docker

There is an image built on Alpine Linux on Docker hub.

## Build

docker buildx build --platform linux/amd64,linux/arm64 -t tinpotnick/rrdb:1.0.2 . --push