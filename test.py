#!/usr/bin/env python3

import os
import time
import re
import random

timevalue = re.compile( r"^(.*)?:(.*)?$", re.MULTILINE | re.IGNORECASE )

# Create a load up with some data, 2 data sets then some xform[ation]s
xforms = "RRDBCOUNT:ONEDAY:"
xforms = xforms + "RRDBCOUNT:FIVEMINUTE:"
xforms = xforms + "RRDBSUM:FIVEMINUTE:0:"
xforms = xforms + "RRDBMAX:FIVEMINUTE:0:"
xforms = xforms + "RRDBMEAN:FIVEMINUTE:0:"
xforms = xforms + "RRDBMEAN:ONEDAY:0:"
xforms = xforms + "RRDBMEAN:FIVEMINUTE:1:"
xforms = xforms + "RRDBMEAN:ONEDAY:1"
os.system( "./rrdb --command=create --dir=./ --filename=test.rrdb --setcount=2 --samplecount=500 --xform=" + xforms )

totalsum = 0
secondtotalsum = 0
totalcount = 1500
ourmax = 0
secondourmax = 0
for n in range( totalcount ):
  value = random.randint( 0, 20000 )
  secondvalue = random.randint( 0, 1000000 )
  totalsum = totalsum + value
  secondtotalsum = secondtotalsum + secondvalue
  ourmax = max( ourmax, value )
  secondourmax = max( secondourmax, secondvalue )
  os.system( "./rrdb --command=update --dir=./ --filename=test.rrdb --value={value}:{secondvalue}".format( value=value, secondvalue=secondvalue ) )

# Check xform 0 - RRDBCOUNT:ONEDAY
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=0" ).read()
firstcountvalue = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 0 - read a count of {count}, expecting {totalcount}".format( count=firstcountvalue, totalcount=totalcount ) )

# Check xform 1 - RRDBCOUNT:FIVEMINUTE
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=1" ).read()
firstcountvalue = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 1 - read a count of {count}, expecting {totalcount}".format( count=firstcountvalue, totalcount=totalcount ) )

# Check xform 2 - RRDBSUM:FIVEMINUTE:0
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=2" ).read()
firsttotalsum = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 2 - read a total sum of {sum}, expecting {totalsum}".format( sum=firsttotalsum, totalsum=totalsum ) )

# Check xform 3 - RRDBMAX:FIVEMINUTE:0
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=3" ).read()
firstmax = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 3 - read a max of {max}, expecting {ourmax}".format( max=firstmax, ourmax=ourmax ) )


# Check xform 4 - RRDBMEAN:FIVEMINUTE:0
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=4" ).read()
meanval = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 4 - read a mean of {meanval}, expecting {ourcalcmean} ({totalsum}/{totalcount})".format( meanval=meanval, ourcalcmean=(totalsum/totalcount), totalsum=totalsum, totalcount=totalcount ) )

# Check xform 5 - RRDBMEAN:ONEDAY:0
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=5" ).read()
meanval = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 5 - read a mean of {meanval}, expecting {ourcalcmean} ({totalsum}/{totalcount})".format( meanval=meanval, ourcalcmean=(totalsum/totalcount), totalsum=totalsum, totalcount=totalcount ) )

# Check xform 6 - RRDBMEAN:FIVEMINUTE:1
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=6" ).read()
meanval = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 6 read a mean of {meanval}, expecting {ourcalcmean} ({totalsum}/{totalcount})".format( meanval=meanval, ourcalcmean=(secondtotalsum/totalcount), totalsum=secondtotalsum, totalcount=totalcount ) )

# Check xform 6 - RRDBMEAN:ONEDAY:1
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=7" ).read()
meanval = float( timevalue.search( readoutput ).group( 2 ).strip() )

print( "Xform 7 read a mean of {meanval}, expecting {ourcalcmean} ({totalsum}/{totalcount})".format( meanval=meanval, ourcalcmean=(secondtotalsum/totalcount), totalsum=secondtotalsum, totalcount=totalcount ) )

print( "Will sleep for 5 minutes to ensure windowing is correct" )
time.sleep( 60 * 5 )

totalcount = 100
totalsum = 0
secondtotalsum = 0
ourmax = 0
for n in range( totalcount ):
  value = random.randint( 0, 20000 )
  secondvalue = random.randint( 0, 1000000 )
  totalsum = totalsum + value
  secondtotalsum = secondtotalsum + secondvalue
  ourmax = max( ourmax, value )
  secondourmax = max( secondourmax, secondvalue )
  os.system( "./rrdb --command=update --dir=./ --filename=test.rrdb --value={value}:{secondvalue}".format( value=value, secondvalue=secondvalue ) )


# Check xform 6 - RRDBMEAN:FIVEMINUTE:1
readoutput = os.popen( "./rrdb --command=fetch --dir=./ --filename=test.rrdb --xform=6" ).read()
vals = timevalue.findall( readoutput )

oldmeanval = float( vals[0][1].strip() )
newmeanval = float( vals[1][1].strip() )

print( "Xform 6 read a mean of {meanval} (last read {oldmeanval}), expecting {ourcalcmean} ({totalsum}/{totalcount})".format( meanval=newmeanval, oldmeanval=oldmeanval, ourcalcmean=(secondtotalsum/totalcount), totalsum=secondtotalsum, totalcount=totalcount ) )