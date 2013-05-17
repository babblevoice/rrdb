

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <time.h>

#include <unistd.h>
#include <getopt.h>
#include <linux/limits.h>

#include<signal.h>

#include <sys/stat.h>
#include <fcntl.h>

#include <string.h>

#include <sys/time.h>


#include "rrdb.h"

/*
 Data manipulation - store and retreive round robin data. Maintain xformations 
 i.e. average, sum, count etc.
 
 Can be run as command or as pipe mode.
 
 Commands are:
 
 -         
 pipe, run the program and wait for input to describe the task with the format:
 
 command filename params (specific to command)
 
 create test.rrdb 0 500 RRDBCOUNT:ONEDAY
 update test.rrdb 0
 
 (or with a set of data)
 create test.rrdb 1 500 RRDBCOUNT:ONEDAY:RRDBCOUNT:FIVEMINUTE:RRDBSUM:FIVEMINUTE:0
 update test.rrdb 4643
 
 To fetch the table of data within the sets:
 fetch test.rrdb
 To fetch a single column table for our xfomrations (in this case the RDBCOUNT:ONEDAY)
 fetch test.rrdb 0
 
 info test.rrdb
 
 create
 Create a new data file, the details about how to store are provided. 
 Set count (how many data sets) sample count(how many historical points 
 to keep). xformations, transformations - storage of averages sums etc.
 
 so from the command line we might run:
 
 rrdb --command=create --dir=data/rrd --filename=nick.rrdb --setcount=0 --samplecount=500 --xform=RRDBCOUNT:ONEDAY
 
 The xformations are:
 RRDBMAX
 RRDBMIN
 RRDBCOUNT
 RRDBMEAN
 RRDBSUM
 
 All xformations will maintain a new data set for each set in the file, apart from RRDBCOUNT, which
 will simply maintain 1 data set for count (i.e. quantity of events).
 and the time spans are:
 
 FIVEMINUTE
 ONEHOUR
 SIXHOUR
 TWELVEHOUR
 ONEDAY
 
 
 update 
 rrdb --command=update --dir=data/rrd --filename=nick.rrdb --value=12
 
 fetch
 
 rrdb --command=fetch --dir=data/rrd --filename=nick.rrdb
 rrdb --command=fetch --dir=data/rrd --filename=nick.rrdb --xform=0
 
 The first will return all of the raw data in columns.
 The second with the param --xform=0 will return xform data set 0 (each set may have different times against rows).
 
 
 */


/*
 Some notes on file locking.
 
 Only two functions can open a file: initRRDBFile and readRRDBFile. Both of which apply a lock 
 at the beggining of the file. Which must be released when the file is closed. Nothing clever
 at the moment, a lock we use as an advisor for the whole file.
 */

/************************************************************************************
 * Function: freeRRDBFile
 *
 * Purpose: free any memory allocations within the RRDB file structure - clean up.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int freeRRDBFile(rrdbFile *fileData)
{
    unsigned int i;
    
    if ( fileData->times )
    {
        free(fileData->times);
    }
    
    for ( i = 0 ; i < fileData->header.setCount; i++ )
    {
         free(fileData->sets[i]);
    }
    
    for ( i = 0 ; i < fileData->xformheader.xformCount; i++ )
    {
        free(fileData->xformdata[i]);
        free(fileData->xformtimes[i]);
    }
    
    return 1;
}

/************************************************************************************
 * Function: printRRDBFile
 *
 * Purpose: output to stdout the data form the sets.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int printRRDBFile(rrdbFile *fileData)
{
    unsigned int i,j;
    unsigned int windowPos = 0;
    
    
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = (i + fileData->header.windowPosition + 1)%fileData->header.sampleCount;
        
        if ( 1 == fileData->times[windowPos].valid )
        {
            printf("%ld.%i", fileData->times[windowPos].time, fileData->times[windowPos].uSecs);
            for ( j = 0 ; j < fileData->header.setCount; j++ )
            {
                printf(":%Lf", fileData->sets[j][windowPos]);
            }
            printf("\n");
        }
    }
    
    return 1;
}

/************************************************************************************
 * Function: printRRDBFileXform
 *
 * Purpose: output to stdout the xfomr data form the sets.
 *
 * Written: 12th March 2013 By: Nick Knight
 ************************************************************************************/
int printRRDBFileXform(rrdbFile *fileData, unsigned int index)
{
    unsigned int i;
    unsigned int windowPos = 0;
    
    if ( index > fileData->xformheader.xformCount )
    {
        printf("ERROR: xform index out of bounds\n");
        return -1;
    }
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        
        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = (i + fileData->xforms[index].windowPosition + 1)%fileData->header.sampleCount;
        
        if ( 1 == fileData->xformtimes[index][windowPos].valid )
        {
            printf("%ld:%Lf\n", fileData->xformtimes[index][windowPos].time, fileData->xformdata[index][windowPos]);
        }
    }
    
    return 1;
}

/************************************************************************************
 * Function: initRRDBFile
 *
 * Purpose: Initialize a file with zeroed out data.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int initRRDBFile(char *filename, unsigned int setCount, unsigned int sampleCount , char *xformations)
{
    int pfd;
	rrdbFile fileData;
	long long totalSizeRequired;
    unsigned int setCountSize;
    
    char *result = NULL;
    const char delims[] = ":";
    
    unsigned int i;
    unsigned int setIndexRequired;
    
	fileData.header.fileVersion = 1;
	fileData.header.windowPosition = 0;
    fileData.header.setCount = setCount;
    fileData.header.sampleCount = sampleCount;
    fileData.xformheader.xformCount = 0;
    
    /* read time data */
    totalSizeRequired = (fileData.header.sampleCount * sizeof (rrdbTimePoint));
    
    fileData.times = malloc(totalSizeRequired);
    memset(fileData.times, 0, totalSizeRequired);
    
    
    /* data sets */
    setCountSize = ( fileData.header.sampleCount * sizeof (rrdbNumber));
    for ( i = 0 ; i < fileData.header.setCount; i++ )
    {
        fileData.sets[i] = malloc(setCountSize);
        memset(fileData.sets[i], 0, setCountSize);
    }
    
    /* now xform data */
    /* xformations takes the format of RRDBCOUNT:ONEHOUR:RRDBCOUNT:ONEDAY:RRDBMEAN:ONEDAY:0 
     for all other items it also takes another param which is the index into the set */
    i = 0;
    result = strtok( xformations, delims );
    while ( result )
    {
        setIndexRequired = FALSE;
        
        if ( 0 == strcmp("RRDBMAX", result))
        {
            fileData.xforms[i].calc = RRDBMAX;
            setIndexRequired = TRUE;
        }
	else if ( 0 == strcmp("RRDBMIN", result))
	{
		fileData.xforms[i].calc = RRDBMIN;
		setIndexRequired = TRUE;
	}
        else if ( 0 == strcmp("RRDBCOUNT", result))
        {
            fileData.xforms[i].calc = RRDBCOUNT;
        }
        else if ( 0 == strcmp("RRDBMEAN", result))
        {
            fileData.xforms[i].calc = RRDBMEAN;
            setIndexRequired = TRUE;
        }
        else if ( 0 == strcmp("RRDBSUM", result))
        {
            fileData.xforms[i].calc = RRDBSUM;
            setIndexRequired = TRUE;
        }
        
        
        
        /* then get the time span */
        result = strtok( NULL, delims );
        if ( NULL == result )
        {
            /* perhaps a major error? */
            printf("ERROR: We really need a time period for the xform (%i)\n", fileData.xforms[i].calc);
            break;
        }
        
        if ( 0 == strcmp("FIVEMINUTE", result))
        {
            fileData.xforms[i].period = FIVEMINUTE;
        }
        else if ( 0 == strcmp("ONEHOUR", result))
        {
            fileData.xforms[i].period = ONEHOUR;
        }
        else if ( 0 == strcmp("SIXHOUR", result))
        {
            fileData.xforms[i].period = SIXHOUR;
        }
        else if ( 0 == strcmp("TWELVEHOUR", result))
        {
            fileData.xforms[i].period = TWELVEHOUR;
        }
        else if ( 0 == strcmp("ONEDAY", result))
        {
            fileData.xforms[i].period = ONEDAY;
        }
        
        fileData.xforms[i].setIndex = 0;
        fileData.xforms[i].windowPosition = 0;
        
        if ( TRUE == setIndexRequired )
        {
            result = strtok( NULL, delims );
            if ( NULL == result )
            {
                /* perhaps a major error? */
                printf("ERROR: We really need an index for the xform\n");
                break;
            }
            fileData.xforms[i].setIndex = atoi(result);
        }
        
        fileData.xformdata[i] = malloc(setCountSize);
        memset(fileData.xformdata[i], 0, setCountSize);
        
        fileData.xformtimes[i] = malloc(totalSizeRequired);
        memset(fileData.xformtimes[i], 0, totalSizeRequired);
        
        i++;
        fileData.xformheader.xformCount = i;
        /* we can repeat until we get all of xforms required */
        
        result = strtok( NULL, delims );
    }
    
    
    if ((pfd = open(filename, O_CREAT | O_RDWR , S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH )) == -1)
    {
        /* failure - should only ouput one error
        printf("ERROR: failed to open file '%s' for writing\n", filename);*/
        return -1;
    }
    
    if ( -1 == lockf(pfd, F_LOCK, 1))
    {
	printf("ERROR: error obtaining lock on file\n");
    }
    
    if ( -1 == writeRRDBFile(pfd, &fileData) )
    {
        printf("ERROR: error writing newly created '%s' on init\n", filename);
        lseek(pfd, 0, SEEK_SET);
        lockf(pfd, F_ULOCK, 1);
        close(pfd);
        return -1;
    }
    freeRRDBFile(&fileData);
    
	return pfd;
}


/************************************************************************************
 * Function: writeRRDBFile
 *
 * Purpose: Write the contents of an RRDB strcture to a file.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int writeRRDBFile(int pfd, rrdbFile *fileData)
{
    long long totalSizeRequired;
    long long setCountSize;
    unsigned int i;
    
    
    lseek(pfd, 0, SEEK_SET);
    
    if ( sizeof(rrdbHeader) != write(pfd, &fileData->header, sizeof(rrdbHeader)) )
	{
        printf("ERROR: failed to write header to file\n");
		return -1;
	}
    
    /* write time points ( and valid flags etc ) */
    totalSizeRequired = ( fileData->header.sampleCount * sizeof (rrdbTimePoint));
    
    if ( totalSizeRequired != write(pfd, fileData->times, totalSizeRequired) )
	{
        printf("ERROR: failed to write time data to file\n");
		return -1;
	}
    
    /* now write data */
    setCountSize = ( fileData->header.sampleCount * sizeof (rrdbNumber));
    
    for ( i = 0 ; i < fileData->header.setCount; i++ )
    {
        if ( setCountSize != write(pfd, fileData->sets[i], setCountSize) )
        {
            printf("ERROR: failed to write time data to file\n");
            return -1;
        }
    }
    
    /* now write our xform data */
    if ( sizeof(rrdbXformsHeader) != write(pfd, &fileData->xformheader, sizeof(rrdbXformsHeader)) )
    {
        printf("ERROR: failed to write xform master header to file\n");
        return -1;
    }
    
    for ( i = 0 ; i < fileData->xformheader.xformCount; i++ )
    {
        if ( sizeof(rrdbXformHeader) != write(pfd, &fileData->xforms[i], sizeof(rrdbXformHeader)) )
        {
            printf("ERROR: failed to write xform header to file\n");
            return -1;
        }
        
        if ( totalSizeRequired != write(pfd, fileData->xformtimes[i], totalSizeRequired) )
        {
            printf("ERROR: failed to write xform time data to file\n");
            return -1;
        }
        
        if ( setCountSize != write(pfd, fileData->xformdata[i], setCountSize) )
        {
            printf("ERROR: failed to write xform data to file\n");
            return -1;
        }
    }
    
    return pfd;
}



/************************************************************************************
 * Function: readRRDBFile
 *
 * Purpose: Read the contents of an RRDB file into memory.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int readRRDBFile(char *filename, rrdbFile *fileData)
{
	int pfd;
	ssize_t amount_read = 0;
    long long totalSizeRequired;
    long long setCountSize;
    unsigned int i;
    
	if ((pfd = open(filename, O_RDWR )) == -1)
	{
		/* failure - should only ouput one error - perhaps need to put this somewhere else?
        	printf("ERROR: failed to open %s for O_RDWR\n", filename);*/
		return -1;
	}
    
    /* we probably could be more clever about this, we lock just a byte to indicate a whole lock. In the future we could probably just lock sections but would it be worth it? */
    lockf(pfd, F_LOCK, 1);
    
	amount_read = read(pfd, &fileData->header, sizeof(rrdbHeader));
    
	if ( sizeof(rrdbHeader) != amount_read )
	{
        printf("ERROR: failed to read a header from %s - there must be one\n", filename);
		/* failure */
		lseek(pfd, 0, SEEK_SET);
        lockf(pfd, F_ULOCK, 1);
        close(pfd);
		return -1;
	}
    
    /* read time data */
    totalSizeRequired = ( fileData->header.sampleCount * sizeof (rrdbTimePoint));
    
    fileData->times = malloc(totalSizeRequired);
    if ( totalSizeRequired != read(pfd, fileData->times, totalSizeRequired) )
    {
        printf("ERROR: failed to read time data from file '%s'\n", filename);
        lseek(pfd, 0, SEEK_SET);
        lockf(pfd, F_ULOCK, 1);
        close(pfd);
        return -1;
    }
    
    
    /* data sets */
    setCountSize = ( fileData->header.sampleCount * sizeof (rrdbNumber));
    for ( i = 0 ; i < fileData->header.setCount; i++ )
    {
        fileData->sets[i] = malloc(setCountSize);
        if ( setCountSize != read(pfd, fileData->sets[i], setCountSize) )
        {
            printf("ERROR: failed to read set data from file '%s'\n", filename);
            lseek(pfd, 0, SEEK_SET);
            lockf(pfd, F_ULOCK, 1);
            close(pfd);
            return -1;
        }
    }
    
    /* now look for xform data 
    fileData->xformheader.xformCount = 0;*/
    if ( sizeof(rrdbXformsHeader) != read(pfd, &fileData->xformheader, sizeof(rrdbXformsHeader)) )
    {
        printf("ERROR: failed to read xform headerfrom file '%s'\n", filename);
        lseek(pfd, 0, SEEK_SET);
        lockf(pfd, F_ULOCK, 1);
        close(pfd);
        return -1;
    }
    
    /* we have some xformations */    
    for ( i = 0 ; i < fileData->xformheader.xformCount; i++ )
    {
        if ( sizeof(rrdbXformHeader) != read(pfd, &fileData->xforms[i], sizeof(rrdbXformHeader)))
        {
            printf("ERROR: failed to read xform header data from file '%s'\n", filename);
            lseek(pfd, 0, SEEK_SET);
            lockf(pfd, F_ULOCK, 1);
            close(pfd);
            return -1;
        }
        
        fileData->xformtimes[i] = malloc(totalSizeRequired);
        if ( totalSizeRequired != read(pfd, fileData->xformtimes[i], totalSizeRequired) )
        {
            printf("ERROR: failed to read xform time data from file '%s'\n", filename);
            lseek(pfd, 0, SEEK_SET);
            lockf(pfd, F_ULOCK, 1);
            close(pfd);
            return -1;
        }
        
        
        fileData->xformdata[i] = malloc(setCountSize);
        if ( setCountSize != read(pfd, fileData->xformdata[i], setCountSize) )
        {
            printf("ERROR: failed to read xform data from file '%s'\n", filename);
            lseek(pfd, 0, SEEK_SET);
            lockf(pfd, F_ULOCK, 1);
            close(pfd);
            return -1;
        }
    }
    
    
	return pfd;
}

/************************************************************************************
 * Function: writeRRDBFile
 *
 * Purpose: Print the info about an RRDB file.
 *
 * Written: 10th March 2013 By: Nick Knight
 ************************************************************************************/
int printRRDBFileInfo(char *filename)
{
    rrdbFile fileData;
    unsigned int i;
    int pfd;
    
    /*
     read in the header to get the window position and make
     sure the set count is correct
     */
    if ( -1 == ( pfd = readRRDBFile(filename, &fileData) ))
    {
        return -1;
    }
    
    printf("Version is %i\n", fileData.header.fileVersion);
	printf("Number of sets %i\n", fileData.header.setCount);
	printf("Number of samples %i\n", fileData.header.sampleCount);
	printf("Current window position %i\n", fileData.header.windowPosition);
    printf("Contains #%i xformations\n", fileData.xformheader.xformCount);
    
    for ( i = 0 ; i < fileData.xformheader.xformCount; i++ )
    {
        switch(fileData.xforms[i].calc)
        {
            case RRDBMAX:
                printf("RRDBMAX:");
                break;
                
            case RRDBMIN:
                printf("RRDBMIN:");
                break;
                
            case RRDBCOUNT:
                printf("RRDBCOUNT:");
                break;
                
            case RRDBMEAN:
                printf("RRDBMEAN:");
                break;
                
            case RRDBSUM:
                printf("RRDBSUM:");
                break;
                
            default:
                break;
        }
        
        switch(fileData.xforms[i].period)
        {
            case FIVEMINUTE:
                printf("FIVEMINUTE\n");
                break;
                
            case ONEHOUR:
                printf("ONEHOUR\n");
                break;
                
            case SIXHOUR:
                printf("SIXHOUR\n");
                break;
                
            case TWELVEHOUR:
                printf("TWELVEHOUR\n");
                break;
                
            case ONEDAY:
                printf("ONEDAY\n");
                break;
                
            default:
                break;
        }
    }

    freeRRDBFile(&fileData);
    lseek(pfd, 0, SEEK_SET);
    lockf(pfd, F_ULOCK, 1);
    close(pfd);
    
    return 1;
}



/************************************************************************************
 * Function: calcRRDBCount
 *
 * Calulate the count of all entrie within a time, count an return them.
 *
 * Written: 11th March 2013 By: Nick Knight
 ************************************************************************************/
rrdbNumber calcRRDBCount(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex)
{
    UNUSED(setIndex);
    rrdbNumber retval = 0;
    unsigned int i;
    
    unsigned int windowPos = 0;
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        /* + 1 so that we loop back round to the start and print them in time order */
        /* thanks http://www.yourdailygeekery.com/2011/06/28/modulo-of-negative-numbers.html - mod negative numbers*/
        
        windowPos = ((fileData->header.windowPosition - i) + fileData->header.sampleCount) % fileData->header.sampleCount;
        
        if ( TRUE == fileData->times[windowPos].valid )
        {
            if ( (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) >= (start->tv_sec + (start->tv_usec/1000000))
                && (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) < (end->tv_sec + (end->tv_usec/1000000)) )
            {
                retval ++;
            }
            else
            {
                /* we store the values in time order so we can bomb out when this happens */
                break;
            }
        }
        else
        {
            break;
        }
    }
    
    
    
    return retval;
}

/************************************************************************************
 * Function: calcRRDBSum
 *
 * Calulate the sum of all entries within a time, count an return them.
 *
 * Written: 11th March 2013 By: Nick Knight
 ************************************************************************************/
rrdbNumber calcRRDBSum(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex)
{
    rrdbNumber retval = 0;
    unsigned int i;
    
    unsigned int windowPos = 0;
    
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = ((fileData->header.windowPosition - i) + fileData->header.sampleCount) % fileData->header.sampleCount;
        
        if ( TRUE == fileData->times[windowPos].valid )
        {
            if ( (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) >= (start->tv_sec + (start->tv_usec/1000000))
                && (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) < (end->tv_sec + (end->tv_usec/1000000)) )
            {
                retval += fileData->sets[setIndex][windowPos];
            }
            else
            {
                /* we store the values in time order so we can bomb out when this happens */
                break;
            }
        }
        else
        {
            break;
        }
    }
    
    return retval;
}

/************************************************************************************
 * Function: calcRRDBMean
 *
 * Calulate the mean of all entries within a time, count an return them.
 *
 * Written: 11th March 2013 By: Nick Knight
 ************************************************************************************/
rrdbNumber calcRRDBMean(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex)
{
    rrdbNumber retval = 0;
    unsigned int i;
    unsigned int count = 0;
    
    unsigned int windowPos = 0;
    
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = ((fileData->header.windowPosition - i) + fileData->header.sampleCount) % fileData->header.sampleCount;
        
        if ( TRUE == fileData->times[windowPos].valid )
        {
            if ( (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) >= (start->tv_sec + (start->tv_usec/1000000))
                && (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) < (end->tv_sec + (end->tv_usec/1000000)) )
            {
                retval += fileData->sets[setIndex][windowPos];
                count++;
            }
            else
            {
                /* we store the values in time order so we can bomb out when this happens */
                break;
            }
        }
        else
        {
            break;
        }
    }
    
    retval = retval/count;
    
    return retval;
}


/************************************************************************************
 * Function: calcRRDBMin
 *
 * Calulate the min of all entries within a time, count an return them.
 *
 * Written: 11th March 2013 By: Nick Knight
 ************************************************************************************/
rrdbNumber calcRRDBMin(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex)
{
    rrdbNumber retval = 0;
    unsigned int i;
    unsigned int firstVal = FALSE;
    
    unsigned int windowPos = 0;
    
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = ((fileData->header.windowPosition - i) + fileData->header.sampleCount) % fileData->header.sampleCount;
        
        if ( TRUE == fileData->times[windowPos].valid )
        {
            if ( (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) >= (start->tv_sec + (start->tv_usec/1000000))
                && (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) < (end->tv_sec + (end->tv_usec/1000000)) )
            {
                if ( !firstVal )
                {
                    retval = fileData->sets[setIndex][windowPos];
                    firstVal = TRUE;
                }
                
                if ( retval > fileData->sets[setIndex][windowPos] )
                {
                    retval = fileData->sets[setIndex][windowPos];
                }
            }
            else
            {
                /* we store the values in time order so we can bomb out when this happens */
                break;
            }
        }
        else
        {
            break;
        }
    }
    
    return retval;
}

/************************************************************************************
 * Function: calcRRDBMax
 *
 * Calulate the max of all entries within a time, count an return them.
 *
 * Written: 11th March 2013 By: Nick Knight
 ************************************************************************************/
rrdbNumber calcRRDBMax(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex)
{
    rrdbNumber retval = 0;
    unsigned int i;
    
    unsigned int windowPos = 0;
    
    
    for ( i = 0 ; i < fileData->header.sampleCount; i++ )
    {
        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = ((fileData->header.windowPosition - i) + fileData->header.sampleCount) % fileData->header.sampleCount;
        
        if ( TRUE == fileData->times[windowPos].valid )
        {
            if ( (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) >= (start->tv_sec + (start->tv_usec/1000000))
                && (fileData->times[windowPos].time + (fileData->times[windowPos].uSecs/1000000)) < (end->tv_sec + (end->tv_usec/1000000)) )
            {
                if ( retval < fileData->sets[setIndex][windowPos] )
                {
                    retval = fileData->sets[setIndex][windowPos];
                }
            }
            else
            {
                /* we store the values in time order so we can bomb out when this happens */
                break;
            }
        }
        else
        {
            break;
        }
    }
    
    return retval;
}


/************************************************************************************
 * Function: updateRRDBFile
 *
 * Purpose: Update a file with the supplied values. At the moment read, modify then
 * write, so could have performance improved by more clever manipulation of the file.
 * It will also ripple the data down to depenant files also.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int updateRRDBFile(char *filename, char* vals)
{
    rrdbFile fileData;
    unsigned int i;
    char *result = NULL;
    const char delims[] = ":";
    struct timeval t1;
    
    struct timeval xformstart, xformend;
    rrdbNumber xformResult;
    
    struct tm *current_tm;
    time_t current_time;
    
    int pfd;
    
    /* 
     read in the header to get the window position and make
     sure the set count is correct
     */
    if ( -1 == ( pfd = readRRDBFile(filename, &fileData) ) )
    {
        return -1;
    }
    
    /* 
     Move round on 1
     */
    fileData.header.windowPosition = ( fileData.header.windowPosition + 1 ) % fileData.header.sampleCount;
    
    /*
     times
     */
    
    fileData.times[fileData.header.windowPosition].valid = 1;
    gettimeofday(&t1, NULL);
    fileData.times[fileData.header.windowPosition].time = t1.tv_sec;
    fileData.times[fileData.header.windowPosition].uSecs = t1.tv_usec;

    
    /*
     The vals are a string of seperated vales in the format of num:num:num
     1 for each set we have in the file.
     */
    
    result = strtok( vals, delims );
    for ( i = 0 ; i < fileData.header.setCount; i++ )
    {
        if ( NULL != result )
        {
            fileData.sets[i][fileData.header.windowPosition] = atof(result);
        }
        else
        {
            fileData.sets[i][fileData.header.windowPosition] = 0;
        }
        result = strtok( NULL, delims );
    }
    
    /*
     * Now we need to update our xformations (xforms) current_tm->tm_hour = 0;
     */
    current_time = t1.tv_sec;
    
    for ( i = 0; i < fileData.xformheader.xformCount; i++)
    {
        current_tm = gmtime(&current_time);
        xformend.tv_usec = 0;
        
        switch (fileData.xforms[i].period) {
            case FIVEMINUTE:
                current_tm->tm_sec = 0;
                /* move to the start of the nearest 5 minutes */
                current_tm->tm_min = ((int)(current_tm->tm_min/5))*5;
                
                
                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                
                xformend.tv_sec = xformstart.tv_sec + ( 60 * 5);
                break;
                
            case ONEHOUR:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                
                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                
                xformend.tv_sec = xformstart.tv_sec + ( 60 * 60);
                break;
                
            case SIXHOUR:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                current_tm->tm_hour = ((int)(current_tm->tm_hour/6))*6;
                
                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                
                xformend.tv_sec = xformstart.tv_sec + ( 60 * 60 * 6);
                break;
                
            case TWELVEHOUR:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                current_tm->tm_hour = ((int)(current_tm->tm_hour/12))*12;
                
                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                
                xformend.tv_sec = xformstart.tv_sec + ( 60 * 60 * 12);
                break;
                
            case ONEDAY:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                current_tm->tm_hour = 0;
                
                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                
                xformend.tv_sec = xformstart.tv_sec + ( 60 * 60 * 24);
                break;
                
            default:
                break;
        }
        
        xformResult = 0;
        switch (fileData.xforms[i].calc) {
            case RRDBMAX:
                xformResult = calcRRDBMax(&xformstart, &xformend, &fileData, fileData.xforms[i].setIndex);
                break;
                
            case RRDBMIN:
                xformResult = calcRRDBMin(&xformstart, &xformend, &fileData, fileData.xforms[i].setIndex);
                break;
                
            case RRDBCOUNT:
                xformResult = calcRRDBCount(&xformstart, &xformend, &fileData, 0);
                break;
                
            case RRDBMEAN:
                xformResult = calcRRDBMean(&xformstart, &xformend, &fileData, fileData.xforms[i].setIndex);
                break;
                
            case RRDBSUM:
                xformResult = calcRRDBSum(&xformstart, &xformend, &fileData, fileData.xforms[i].setIndex);
                break;
                
            default:
                break;
        }
        
        /*
         The value should be placed in the current windowed position, if still valid (i.e. updated)
         or if it is now outside the time window moved on 1.
         */
        if( fileData.xformtimes[i][fileData.xforms[i].windowPosition].time != xformstart.tv_sec )
        {
            /* we need to move on the window... */
            fileData.xforms[i].windowPosition = (fileData.xforms[i].windowPosition + 1 ) % fileData.header.sampleCount;
        }
        fileData.xformdata[i][fileData.xforms[i].windowPosition] = xformResult;
        
        fileData.xformtimes[i][fileData.xforms[i].windowPosition].time = xformstart.tv_sec;
        fileData.xformtimes[i][fileData.xforms[i].windowPosition].uSecs = 0;
        fileData.xformtimes[i][fileData.xforms[i].windowPosition].valid = TRUE;
    }
    
    /*
     Now write it 
     */
    if ( -1 == writeRRDBFile(pfd, &fileData) )
    {
        printf("ERROR: could no write data to RRDB file\n");
        freeRRDBFile(&fileData);
        lseek(pfd, 0, SEEK_SET);
        lockf(pfd, F_ULOCK, 1);
        close(pfd);
        return -1;
        
    }
    
    
    freeRRDBFile(&fileData);
    lseek(pfd, 0, SEEK_SET);
    lockf(pfd, F_ULOCK, 1);
    close(pfd);
    return 1;
}


/************************************************************************************
 * Function: runCommand
 *
 * Purpose: runs the command
 *
 * Written: 10th March 2013 By: Nick Knight
 ************************************************************************************/
int runCommand(char *filename, RRDBCommand ourCommand, unsigned int sampleCount, unsigned int setCount, char *values, char *xformations)
{
    rrdbFile ourFile;
    int pfd;
    
    switch(ourCommand)
    {
        case CREATE:
            if ( 0 >= sampleCount )
            {
                printf("ERROR: sample count too small, must be more than zero.\n");
                return -1;
            }
            
            if ( -1 == ( pfd = initRRDBFile(filename, setCount, sampleCount, xformations)) )
            {
                printf("ERROR: writing db file error");
            }
            lseek(pfd, 0, SEEK_SET);
            lockf(pfd, F_ULOCK, 1);
            close(pfd);
            break;
            
        case FETCH:
            if ( -1 == ( pfd = readRRDBFile(filename, &ourFile)) )
            {
                printf("ERROR: failed to read rrdb file '%s'\n", filename);
                return -1;
            }
            
            if ( 0 != strlen(xformations) )
            {
                if ( -1 == printRRDBFileXform(&ourFile, atoi(xformations)))
                {
                    printf("ERROR: failed to retreive rrdb data\n");
                }
            }
            else
            {
                if ( -1 == printRRDBFile(&ourFile))
                {
                    printf("ERROR: failed to retreive rrdb data\n");
                }
            }
            
            freeRRDBFile(&ourFile);
            lseek(pfd, 0, SEEK_SET);
            lockf(pfd, F_ULOCK, 1);
            close(pfd);
            
            break;
            
        case UPDATE:
            /* we should be given a value for each set we have */
            if ( -1 ==  updateRRDBFile(filename, &values[0]))
            {
                printf("ERROR: failed to update rrdb data\n");
                return -1;
            }
            
            break;
            
        case INFO:
            printRRDBFileInfo(filename);
            
            break;
            
        case PIPE:
            break;
    }
    
    return 1;
}

/************************************************************************************
 * Function: waitForInput
 *
 * Purpose: Wait for input from stdin and parse and run command. The string should be
 * in the format of 
 * command filename 
 * Then for fetch no more
 * for update a string of values in the format val:val:val for the string of values
 * for create setCount sampleCount 
 *
 * Written: 10th March 2013 By: Nick Knight
 ************************************************************************************/
int waitForInput(char *dir)
{
    
    char c;
    char command[MAXCOMMANDLENGTH];
    RRDBCommand ourCommand;
    const char delims[] = " ";
    unsigned int i = 0;
    int pathlength = 0;
    unsigned int sampleCount = 0;
    unsigned int setCount = 0;
    char values[MAXVALUESTRING];
    values[0] = 0;
    char *result = NULL;
    char xformations[MAXVALUESTRING];
    xformations[0] = 0;
    
    char fulldirname[PATH_MAX + NAME_MAX];
    
    command[0] = 0;
    while ( ( c = fgetc(stdin)) != '\n' )
    {
        if ( c == '\r' )
        {
            continue;
        }
        command[i] = c;
        command[i+1] = 0;
        
        if (i > MAXCOMMANDLENGTH - 5)
        {
            printf("ERROR: command too long\n");
            exit(1);
        }
        
        i++;
    }
    
    if ( 0 == strlen(command))
    {
        return -1;
    }
    
    /* command */
    result = strtok( command, delims );
    if ( 0 == strcmp("create", result) )
    {
        ourCommand = CREATE;
    }
    else if ( 0 == strcmp("update", result) )
    {
        ourCommand = UPDATE;
    }
    else if ( 0 == strcmp("fetch", result) )
    {
        ourCommand = FETCH;
    }
    else if ( 0 == strcmp("info", result) )
    {
        ourCommand = INFO;
    }
    else
    {
        /* we must have a command */
        printf("ERROR: no valid command so quiting\n");
        exit(1);
    }

    /* filename */
    strcpy(&fulldirname[0], &dir[0]);
    pathlength = strlen(dir);
    fulldirname[pathlength] = '/';
    pathlength++;
    fulldirname[pathlength] = 0;
    
    result = strtok( NULL, delims );
    strcpy(&fulldirname[pathlength], result);


    /* setcount or values */
    result = strtok( NULL, delims );
    if ( NULL != result )
    {
        if ( CREATE == ourCommand )
        {
            setCount = atoi(result);
        }
        else if ( FETCH == ourCommand )
        {
            if ( strlen(result) > MAXVALUESTRING )
            {
                printf("ERROR: Length of xformations string too long\n");
                exit(1);
            }
            strcpy( &xformations[0], result );
        }
        else
        {
            if ( strlen(result) > MAXVALUESTRING )
            {
                printf("ERROR: Length of value string too long\n");
                exit(1);
            }
            strcpy( &values[0], result );
        }
    }

    /* samplecount */
    result = strtok( NULL, delims );
    if ( NULL != result )
    {
        sampleCount = atoi(result);
    }
    
    if ( CREATE == ourCommand )
    {
        result = strtok( NULL, delims );
        if ( NULL != result )
        {
            if (strlen(result) > MAXVALUESTRING)
            {
                printf("ERROR: Length of xformation string too long\n");
                exit(1);

            }
            strcpy( &xformations[0], result );
        }
    }

    
    if ( -1 != runCommand(fulldirname, ourCommand, sampleCount, setCount, values, xformations) )
    {
        printf("OK\n");
    }
    
    return 1;
}

/************************************************************************************
 * Function: sigHandler
 *
 * Purpose: Handle UNIX signals.
 *
 * Written: 13th March 2013 By: Nick Knight
 ************************************************************************************/
static void sigHandler(int signo)
{
    if (signo == SIGINT)
    {
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        exit(0);
    }
}

/************************************************************************************
 * Function: main
 *
 * Purpose: Parse command line, kick off whatever the user requested.
 *
 * Written: 9th March 2013 By: Nick Knight
 ************************************************************************************/
int main(int argc, char **argv)
{
	char c;
    RRDBCommand ourCommand;
    
    unsigned int setCount = 0;
    unsigned int sampleCount = 0;
    
    char dir[PATH_MAX];
    char fulldirname[PATH_MAX + NAME_MAX];
    
    char filename[NAME_MAX];
    int pathlength = 0;
    
    char values[MAXVALUESTRING];
    char xformations[MAXVALUESTRING];    
    xformations[0] = 0;
    
    static struct option long_options[] = {
        {"command",     1, 0, 0 },
        {"setcount",    1, 0, 1 },
        {"samplecount", 1, 0, 2 },
        {"dir",         1, 0, 3 },
        {"filename",    1, 0, 4 },
        {"values",      1, 0, 5 },
        {"xform",       1, 0, 6 },
        {0,             0, 0, 7 }
    };
    
    int option_index = 0;
    
    setvbuf(stdin, NULL, _IOLBF, 0);
    setvbuf(stdout, NULL, _IOLBF, 0);
    
    memset(&dir[0], 0, PATH_MAX);
    memset(&filename[0], 0, NAME_MAX);
    memset(&values[0], 0, MAXVALUESTRING);
    
    if (signal(SIGINT, sigHandler) == SIG_ERR)
    {
        printf("ERROR: can't catch SIGINT\n");
        exit(1);
    }
    
    /* default */
    ourCommand = PIPE;
    
	while ((c = getopt_long(argc, argv, "",
                            long_options, &option_index)) != -1)
	{
		switch (c)
		{
                
            case 0:
                if ( 0 == strcmp("-", optarg) )
                {
                    ourCommand = PIPE;
                }
                else if ( 0 == strcmp("create", optarg) )
                {
                    ourCommand = CREATE;
                }
                else if ( 0 == strcmp("update", optarg) )
                {
                    ourCommand = UPDATE;
                }
                else if ( 0 == strcmp("fetch", optarg) )
                {
                    ourCommand = FETCH;
                }
                else if ( 0 == strcmp("info", optarg) )
                {
                    ourCommand = INFO;
                }
                
                break;
            case 1:
                setCount = atoi(optarg);;
                break;
                
            case 2:
                sampleCount = atoi(optarg);;
                break;
                
            case 3:
                /* directory */
                if ( strlen(optarg) > PATH_MAX )
                {
                    printf("ERROR: Length of path too long\n");
                    exit(1);
                }
                strcpy( &dir[0], optarg );
                
                break;
                
            case 4:
                /* filename */
                if ( strlen(optarg) > NAME_MAX )
                {
                    printf("ERROR: Length of filename too long\n");
                    exit(1);
                }
                strcpy( &filename[0], optarg );
                
            case 5:
                /* values */
                if ( strlen(optarg) > MAXVALUESTRING )
                {
                    printf("ERROR: Length of value string too long\n");
                    exit(1);
                }
                
                strcpy( &values[0], optarg );
                
                break;
                
            case 6:
                /* xformations */
                strcpy( &xformations[0], optarg );
                
                break;
                
            default:
                /* Unknown option */
                exit(1);
		}
	}
    
    if ( PIPE == ourCommand )
    {
        while(-1 != waitForInput(dir));
    }
    else
    {
        strcpy(&fulldirname[0], &dir[0]);
        pathlength = strlen(dir);
        fulldirname[pathlength] = '/';
        fulldirname[pathlength+1] = 0;
        pathlength++;
        
        strcpy(&fulldirname[pathlength], &filename[0]);
        
        runCommand(fulldirname, ourCommand, sampleCount, setCount, values, xformations);

    }
    
    /* mainly to keep users of valgrind happy as to while 3 file descriptors are still open */
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
    
	return 0;
}



