

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <time.h>

#include <unistd.h>
#include <getopt.h>
#include <signal.h>

#include <sys/stat.h>
#include <fcntl.h>

#include <sys/time.h>
#include <errno.h>
#include <sys/mman.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#ifndef NAME_MAX
#define NAME_MAX 4096
#endif

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

 V2 Touch
 Records a count against a path. The path is comma delimitered, so that
 it will record a touch against the whole touchpath and also each item
 which is comma delimitered.

 truncate: the number of sets after which it will start to re-use older non used sets
 touchpath: the string we are counting against.

 rrdb --command=touch --dir=data/rrd --filename=nick.rrdb --touchpath=test
 rrdb --command=touch --dir=data/rrd --filename=nick.rrdb --touchpath=tech,support --samplecount=2000 --period=ONEHOUR,ONEDAY --setcount=50

 In pipe mode, these are the equivalent commands
 touch test.rrdb 50 2000 tech,support ONEHOUR,ONEDAY
 touch <filename> <setcount> <samplecount> <path> <period>

 (setcount could also be described as max setcount)
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
 * Function: printRRDBTouchFile
 *
 * Purpose: Output the data formated.
 *
 * Written: 19th April 2017 By: Nick Knight
 ************************************************************************************/
int printRRDBTouchFile(int pfd, char * path, char * period)
{
  char *addr, *ptr;
  struct stat sb;
  rrdbInt *values, value;
  unsigned int iperiod;

  rrdbTouchHeader *header;
  rrdbTouchSet *setHeader;
  unsigned int i,j, timepersample, missingsamples, outputsamples, nowindex;
  rrdbTimeEpochSeconds sampleTime;
  time_t now, missing;

  if ( fstat( pfd, &sb ) == -1 )           /* To obtain file size */
  {
    return -1;
  }

  if ( 0 == strcmp( period, "FIVEMINUTE" ) )
  {
    iperiod = FIVEMINUTE;
  }
  else if ( 0 == strcmp( period, "ONEHOUR" ) )
  {
    iperiod = ONEHOUR;
  }
  else if ( 0 == strcmp( period, "SIXHOUR" ) )
  {
    iperiod = SIXHOUR;
  }
  else if ( 0 == strcmp( period, "TWELVEHOUR" ) )
  {
    iperiod = TWELVEHOUR;
  }
  else if ( 0 == strcmp( period, "ONEDAY" ) )
  {
    iperiod = ONEDAY;
  }
  else
  {
    iperiod = ONEHOUR;
  }

  addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, pfd, 0);

  if (addr == MAP_FAILED)
  {
    return -1;
  }

  header = ( rrdbTouchHeader * ) addr;
  ptr = addr + sizeof( rrdbTouchHeader );

  for ( i = 0 ; i < header->sets; i++ )
  {
    setHeader = ( rrdbTouchSet * ) ptr;

    if ( 0 != path[ 0 ] && 0 != strcmp( setHeader->path, path ) )
    {
      ptr += sizeof( rrdbTouchSet ) + ( header->samplesPerSet * sizeof( rrdbInt ) );
      continue;
    }

    if ( setHeader->period != iperiod )
    {
      ptr += sizeof( rrdbTouchSet ) + ( header->samplesPerSet * sizeof( rrdbInt ) );
      continue;
    }

    timepersample = getTimePerSample( setHeader->period );
    //printf( "H:%s:%i\n", setHeader->path, timepersample );

    /* Round it up to the nearest time block */
    sampleTime = setHeader->lastTouch / timepersample;
    sampleTime = ( sampleTime * timepersample ) + timepersample;

    now = time( NULL ) / timepersample;
    now = ( now * timepersample ) + timepersample;

    missing = now - sampleTime;
    missingsamples = missing / timepersample;

    if ( missingsamples >= header->samplesPerSet )
    {
      outputsamples = 0;
    }
    else
    {
      outputsamples = header->samplesPerSet - missingsamples;
    }
    values = ( rrdbInt * ) ( ptr + sizeof( rrdbTouchSet ) );
    for ( j = 0; j < outputsamples; j++ )
    {
      nowindex = ( ( sampleTime / timepersample ) + 1 ) % header->samplesPerSet;

      value = values[ nowindex ];
      if ( 0 != value )
      {
        printf("%ld:%i\n", sampleTime, values[ nowindex ] );
      }
      sampleTime -= timepersample;
    }
    break;
  }

  munmap( addr, sb.st_size );

  return 0;
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

    if ( index >= fileData->xformheader.xformCount ) {
        printf("ERROR: xform index out of bounds\n");
        return -1;
    }

    for ( i = 0 ; i < fileData->header.sampleCount; i++ ) {

        /* + 1 so that we loop back round to the start and print them in time order */
        windowPos = (i + fileData->xforms[index].windowPosition + 1)%fileData->header.sampleCount;

        if ( 1 == fileData->xformtimes[index][windowPos].valid ) {
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

  fileData.header.fileVersion = RRDBV1;
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

  if ( -1 == writeRRDBFile(pfd, &fileData) ) {
      lseek(pfd, 0, SEEK_SET);
      if( 0 != lockf(pfd, F_ULOCK, 1) ) {
        fprintf( stderr, "Failed to unlock file\n" );
      }
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
int readRRDBFile(int pfd, rrdbFile *fileData)
{
  ssize_t amount_read = 0;
  long long totalSizeRequired;
  long long setCountSize;
  unsigned int i;

  amount_read = read(pfd, &fileData->header, sizeof(rrdbHeader));

  if ( sizeof(rrdbHeader) != amount_read )
  {
      printf("ERROR: failed to read a RRDB header - there must be one??\n");
      return -1;
  }

  /* read time data */
  totalSizeRequired = ( fileData->header.sampleCount * sizeof (rrdbTimePoint));

  fileData->times = malloc(totalSizeRequired);
  if ( totalSizeRequired != read(pfd, fileData->times, totalSizeRequired) ) {
      printf("ERROR: failed to read time data from RRDB file\n");
      return -1;
  }


  /* data sets */
  setCountSize = ( fileData->header.sampleCount * sizeof (rrdbNumber));
  for ( i = 0 ; i < fileData->header.setCount; i++ ) {
      fileData->sets[i] = malloc(setCountSize);
      if ( setCountSize != read(pfd, fileData->sets[i], setCountSize) ) {
          printf("ERROR: failed to read set data from RRDB file\n");
          return -1;
      }
  }

  /* now look for xform data
  fileData->xformheader.xformCount = 0;*/
  if ( sizeof(rrdbXformsHeader) != read(pfd, &fileData->xformheader, sizeof(rrdbXformsHeader)) ) {
      printf("ERROR: failed to read xform header from RRDB file\n");
      return -1;
  }

  /* we have some xformations */
  for ( i = 0 ; i < fileData->xformheader.xformCount; i++ ) {
      if ( sizeof(rrdbXformHeader) != read(pfd, &fileData->xforms[i], sizeof(rrdbXformHeader))) {
          printf("ERROR: failed to read xform header data from RRDB file\n");
          return -1;
      }

      fileData->xformtimes[i] = malloc(totalSizeRequired);
      if ( totalSizeRequired != read(pfd, fileData->xformtimes[i], totalSizeRequired) ) {
          printf("ERROR: failed to read xform time data from RRDB file\n");
          return -1;
      }


      fileData->xformdata[i] = malloc(setCountSize);
      if ( setCountSize != read(pfd, fileData->xformdata[i], setCountSize) )
      {
          printf("ERROR: failed to read xform data from RRDB file\n");
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

    if ((pfd = open(filename, O_RDWR )) == -1) {
      printf("ERROR: failed to open %s for O_RDWR\n", filename);
      return -1;
    }

    /* Lock */
    if( 0 != lockf(pfd, F_LOCK, 1) ) {
      close(pfd);
      printf("ERROR: failed to acquire lock for %s\n", filename);
      return -1;
    }

    if ( RRDBTOUCHV2 == getFileVersion( pfd ) )
    {
      rrdbTouchHeader *ourtouchheader;
      rrdbTouchSet *setHeader;
      struct stat sb;
      char *addr, *ptr;
      unsigned int loopcount;

      if ( fstat( pfd, &sb ) == -1 ) { /* To obtain file size */
        printf("ERROR: cannot stat RRDB file\n");
        lseek(pfd, 0, SEEK_SET);
        if( 0 != lockf(pfd, F_ULOCK, 1) ) {
          fprintf( stderr, "Failed to unlock file\n" );
        }
        close(pfd);
        return -1;
      }

      addr = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, pfd, 0);
      if (addr == MAP_FAILED) {
        printf("ERROR: error accessing data file.\n");
        lseek(pfd, 0, SEEK_SET);
        if( 0 != lockf(pfd, F_ULOCK, 1) ) {
          fprintf( stderr, "Failed to unlock file\n" );
        }
        close(pfd);
        return -1;
      }

      ourtouchheader = ( rrdbTouchHeader * ) addr;
      setHeader = ( rrdbTouchSet * )( addr + sizeof(rrdbTouchHeader) );

      printf( "2:%i:%i\n", ourtouchheader->sets, ourtouchheader->samplesPerSet );

      for ( loopcount = 0; loopcount < ourtouchheader->sets; loopcount++ ) {
        printf("%s:%i\n", setHeader->path, getTimePerSample( setHeader->period ) );
        ptr = (char *)setHeader;
        ptr += sizeof( rrdbTouchSet ) + ( ourtouchheader->samplesPerSet * sizeof( rrdbInt ) );
        setHeader = ( rrdbTouchSet * ) ptr;
      }

      munmap( ( char * ) addr, sb.st_size );

      lseek(pfd, 0, SEEK_SET);
      if( 0 != lockf(pfd, F_ULOCK, 1) ) {
        fprintf( stderr, "Failed to unlock file\n" );
      }

      close(pfd);
      return 1;
    }

    if( -1 == readRRDBFile(pfd, &fileData) ) {
      lseek(pfd, 0, SEEK_SET);
      if( 0 != lockf(pfd, F_ULOCK, 1) ) {
        fprintf( stderr, "Failed to unlock file\n" );
      }
      close(pfd);
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
    if( 0 != lockf(pfd, F_ULOCK, 1) ) {
      fprintf( stderr, "Failed to unlock file\n" );
    }
    close(pfd);

    return 1;
}

/************************************************************************************
 * Function: modifyRRDBFile
 * Written: 22nd June 2021 By: Nick Knight
 ************************************************************************************/
int modifyRRDBFile(char *filename, char* vals, char* xform)
{
    rrdbFile fileData;
    int ixform;
    int pfd;

    if ((pfd = open(filename, O_RDWR )) == -1) {
      printf("ERROR: failed to open %s for O_RDWR\n", filename);
      return -1;
    }

    /* Lock */
    if( 0 != lockf(pfd, F_LOCK, 1) ) {
      close(pfd);
      printf("ERROR: Failed to acquire lock for %s\n", filename);
      return -1;
    }

    if ( RRDBTOUCHV2 == getFileVersion( pfd ) ) {
      printf("Unsupported version V2\n");
      goto finishmodify;
    }

    if( -1 == readRRDBFile(pfd, &fileData) ) {
      lseek(pfd, 0, SEEK_SET);
      if( 0 != lockf(pfd, F_ULOCK, 1) ) {
        fprintf( stderr, "Failed to unlock file\n" );
      }
      close(pfd);
      return -1;
    }

    printf("Version is %i\n", fileData.header.fileVersion);
    printf("Number of sets %i\n", fileData.header.setCount);
    printf("Number of samples %i\n", fileData.header.sampleCount);
    printf("Current window position %i\n", fileData.header.windowPosition);
    printf("Contains #%i xformations\n", fileData.xformheader.xformCount);

    // vals == "time:val" i.e. "1234:111"
    // OR
    // 1234.33:111 - raw data has a usec component also
    char *token;
    time_t indextime = 0;
    rrdbTimemSeconds usec = 0;

    if( NULL != strchr( vals, '.' ) )
    {
        token = strtok( vals, "." );
        indextime = atol( token );

        token = strtok( NULL, ":" );
        usec = atoi( token );
    }
    else
    {
        token = strtok( vals, ":" );
        indextime = atol( token );
    }
    token = strtok( NULL, ":" );
    long double newvalue = atof( token );

    if( strlen(xform) > 0 )
    {
      ixform = atoi(xform);
      printf("Modifying xform %i, time %ld, new value %Lf\n", ixform, indextime, newvalue );

      if( ixform > fileData.xformheader.xformCount )
      {
        printf("xform out of range\n");
        goto finishmodify;
      }

      for ( int i = 0 ; i < fileData.header.sampleCount; i++ )
      {
        if( indextime == fileData.xformtimes[ixform][i].time )
        {
          printf("Modifying %ld:%Lf\n", fileData.xformtimes[ixform][i].time, fileData.xformdata[ixform][i] );
          fileData.xformdata[ixform][i] = newvalue;
          goto finishmodify;
        }
      }
      goto finishnomodify;
    }

    printf("Modifying raw data, time %ld.%i, with new value %Lf\n", indextime, usec, newvalue );
    for ( int i = 0 ; i < fileData.header.sampleCount; i++ ) {
      if ( 1 == fileData.times[i].valid ) {
        if( indextime == fileData.times[i].time &&
            usec == fileData.times[i].uSecs ) {
          for ( int j = 0 ; j < fileData.header.setCount; j++ ) {
            printf("Modifying raw data time %ld.%i ;old value %Lf; new value %Lf\n", fileData.times[i].time, fileData.times[i].uSecs, fileData.sets[j][i], newvalue );
            fileData.sets[j][i] = newvalue;
          }
          goto finishmodify;
        }
      }
    }

    goto finishnomodify;

finishmodify:

    if ( -1 == writeRRDBFile(pfd, &fileData) )
      fprintf( stderr, "Could not write data to RRDB file\n");

finishnomodify:

    freeRRDBFile(&fileData);
    lseek(pfd, 0, SEEK_SET);
    if( 0 != lockf(pfd, F_ULOCK, 1) ) {
      fprintf( stderr, "Failed to unlock file\n" );
    }
    close(pfd);

    return 1;
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

    struct timeval xformstart;
    rrdbNumber xformResult;

    struct tm *current_tm;
    time_t current_time;

    int pfd;

    xformstart = (struct timeval){0};

    if ((pfd = open(filename, O_RDWR )) == -1) {
      printf("ERROR: failed to open %s for O_RDWR\n", filename);
      return -1;
    }

    /* Lock */
    if( 0 != lockf(pfd, F_LOCK, 1) ) {
      close(pfd);
      printf("ERROR: failed to acqure lock for %s\n", filename);
      return -1;
    }

    /*
     read in the header to get the window position and make
     sure the set count is correct
     */
    if( -1 == readRRDBFile(pfd, &fileData) ) {
      lseek(pfd, 0, SEEK_SET);
      if( 0 != lockf(pfd, F_ULOCK, 1) ) {
        fprintf( stderr, "Failed to unlock file\n" );
      }
      close(pfd);
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
    for ( i = 0 ; i < fileData.header.setCount; i++ ) {
        if ( NULL != result )
          fileData.sets[i][fileData.header.windowPosition] = atof(result);
        else
          fileData.sets[i][fileData.header.windowPosition] = 0;

        result = strtok( NULL, delims );
    }

    /*
     * Now we need to update our xformations (xforms) current_tm->tm_hour = 0;
     */
    current_time = t1.tv_sec;

    for ( i = 0; i < fileData.xformheader.xformCount; i++) {
        current_tm = gmtime(&current_time);

        switch (fileData.xforms[i].period) {
            case FIVEMINUTE:
                current_tm->tm_sec = 0;
                /* move to the start of the nearest 5 minutes */
                current_tm->tm_min = ((int)(current_tm->tm_min/5))*5;


                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                break;

            case ONEHOUR:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;

                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                break;

            case SIXHOUR:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                current_tm->tm_hour = ((int)(current_tm->tm_hour/6))*6;

                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                break;

            case TWELVEHOUR:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                current_tm->tm_hour = ((int)(current_tm->tm_hour/12))*12;

                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                break;

            case ONEDAY:
                current_tm->tm_sec = 0;
                current_tm->tm_min = 0;
                current_tm->tm_hour = 0;

                xformstart.tv_sec = mktime(current_tm);
                xformstart.tv_usec = 0;
                break;

            default:
                break;
        }

        xformResult = 0;

        /*
         The value should be placed in the current windowed position, if still valid (i.e. updated)
         or if it is now outside the time window moved on 1.
         */
        unsigned int writeWindowPosition = fileData.xforms[i].windowPosition;
        int movedon = FALSE;
        if( fileData.xformtimes[i][fileData.xforms[i].windowPosition].time != xformstart.tv_sec ) {
            /* we need to move on the window... */
            writeWindowPosition = (fileData.xforms[i].windowPosition + 1 ) % fileData.header.sampleCount;
            movedon = TRUE;
        }

        switch (fileData.xforms[i].calc) {
            case RRDBMAX:
                if( TRUE == movedon )
                  xformResult = fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition];
                else
                  xformResult = MAX( fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition],
                                     fileData.xformdata[i][writeWindowPosition] );
                break;

            case RRDBMIN:
                if( TRUE == movedon )
                  xformResult = fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition];
                else
                  xformResult = MIN( fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition],
                                     fileData.xformdata[i][writeWindowPosition] );
                break;

            case RRDBCOUNT:
                if( TRUE == movedon )
                  xformResult = 1;
                else
                  xformResult = fileData.xformdata[i][writeWindowPosition] + 1;

                break;

            case RRDBMEAN:
            {
                unsigned int countWindowPosition = (writeWindowPosition + 1) % fileData.header.sampleCount;
                if( TRUE == movedon ) {
                  /* We use the next slot to store our running count so we can add to the average - and hide it */
                  fileData.xformtimes[i][countWindowPosition].valid = FALSE;
                  fileData.xformdata[i][countWindowPosition] = 1;
                  xformResult = fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition];
                } else {
                  rrdbNumber countinmean = fileData.xformdata[i][countWindowPosition];
                  if( countinmean <= 0 ) countinmean = 1; /* allow for corruption */
                  rrdbNumber reversemean = fileData.xformdata[i][writeWindowPosition] * countinmean;
                  rrdbNumber newval = fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition];
                  xformResult = ( reversemean + newval ) /
                                ( countinmean + 1 );

                  fileData.xformdata[i][countWindowPosition]++;
                }
                break;
            }
            case RRDBSUM:
                if( TRUE == movedon )
                  xformResult = fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition];
                else
                  xformResult = fileData.sets[fileData.xforms[i].setIndex][fileData.header.windowPosition] +
                                     fileData.xformdata[i][writeWindowPosition];
                break;

            default:
                break;
        }

        fileData.xformdata[i][writeWindowPosition] = xformResult;
        fileData.xformtimes[i][writeWindowPosition].time = xformstart.tv_sec;
        fileData.xformtimes[i][writeWindowPosition].uSecs = 0;
        fileData.xformtimes[i][writeWindowPosition].valid = TRUE;
        fileData.xforms[i].windowPosition = writeWindowPosition;
    }

    /*
     Now write it
     */
    int retval = 1;
    if ( -1 == writeRRDBFile(pfd, &fileData) ) retval = -1;


    freeRRDBFile(&fileData);
    lseek(pfd, 0, SEEK_SET);
    if( 0 != lockf(pfd, F_ULOCK, 1) ) {
      fprintf( stderr, "Failed to unlock file\n" );
    }
    close(pfd);
    return retval;
}

/************************************************************************************
 * Function: getFileVersion
 *
 * Purpose: Load the first Int in a file which indicates which file type.
 *
 * Written: 19th April 2017 By: Nick Knight
 ************************************************************************************/
int getFileVersion(int pfd)
{
  int version;

  off_t curpos = lseek( pfd, 0, SEEK_CUR );

  lseek( pfd, 0, SEEK_SET );
  if( -1 == read( pfd, &version, sizeof(version) ) ) {
    fprintf( stderr, "Failed to read file in getFileVersion\n");
    return -1;
  }
  lseek( pfd, curpos, SEEK_SET );
  return version;
}

/************************************************************************************
 * Function: getTimePerSample
 *
 * Purpose: Get the number of samples per period.
 *
 * Written: 19th April 2017 By: Nick Knight
 ************************************************************************************/
unsigned int getTimePerSample(unsigned int period)
{
  switch( period )
  {
    case FIVEMINUTE:
      return 60 * 5;
      break;

    case ONEHOUR:
      return 60 * 60;
      break;

    case SIXHOUR:
      return 60 * 60 * 6;
      break;

    case TWELVEHOUR:
      return 60 * 60 * 12;
      break;

    case ONEDAY:
      return 60 * 60 * 24;
      break;
  }

  /* Default */
  return 60 * 60 * 24;
}

/************************************************************************************
 * Function: touchSet
 *
 * Purpose: We have a set to update, clear some variables back to zero, and the
 * latest increment. The file pointer is expected to be at the start of the
 * setdata.
 *
 * Written: 8th April 2017 By: Nick Knight
 ************************************************************************************/
int touchSet( rrdbTouchHeader *header, rrdbTouchSet *setHeader, rrdbInt *setdata)
{
  time_t timePerSample = getTimePerSample( setHeader->period );

  /* Work out how many samples we need to clear out. */
  time_t now = time(NULL);

  unsigned int nowindex = ( now / timePerSample ) % header->samplesPerSet;
  unsigned int lastindex = ( setHeader->lastTouch / timePerSample ) % header->samplesPerSet;
  unsigned int numtoclear = ( now / timePerSample ) - ( setHeader->lastTouch / timePerSample );

  if ( numtoclear > 1 )
  {
    /*
     numtoclear actually starts out as the distance between to
     2 samples. The numtoclear is the space between the 2.
    */
    numtoclear = numtoclear - 1;
    if ( numtoclear > header->samplesPerSet )
    {
      /* Clear the set */
      /* We need some zeros */
      memset( setdata, 0, header->samplesPerSet * sizeof(rrdbInt) );
    }
    else if ( nowindex > lastindex )
    {
      /*
        Our valid data range is  within a continuous range. We have
        to clear eather side (2 blocks) of data.
      */
      memset( setdata, 0, nowindex * sizeof(rrdbInt) );
      memset( setdata + lastindex + 1, 0, ( header->samplesPerSet - lastindex - 1 ) * sizeof(rrdbInt) );
    }
    else
    {
      /*
        Our valid data range is within a continuous range. We have
        one contiguous block of data to clear out.
      */
      memset( setdata + lastindex + 1, 0, numtoclear * sizeof(rrdbInt) );
    }
  }

  /* Now update the set */
  setdata[ nowindex ]++;
  return 1;
}

/************************************************************************************
 * Function: findTouchSet
 *
 * Purpose: Search through exsisting sets to find the one of interest - or if not
 * found either create a new one up to our max or overwrite the oldest one untouched.
 *
 * Written: 8th April 2017 By: Nick Knight
 ************************************************************************************/
int findTouchSet(int pfd, char *path, unsigned int period, unsigned int maxsets)
{
  rrdbTouchHeader *header;
  unsigned int samplesPerSet;
  rrdbTouchSet *setHeader, *oldestHeader = NULL;
  unsigned int loopcount;
  rrdbInt *setdata;
  struct stat sb;
  char *addr, *ptr;
  int retval = -1;

  if ( fstat( pfd, &sb ) == -1 ) { /* To obtain file size */
    printf("ERROR: failed to stat file\n");
    return -1;
  }

  if ( 0 == strlen( path ) ) {
    printf("ERROR: path should be a string\n");
    return -1;
  }

  addr = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, pfd, 0);
  if (addr == MAP_FAILED)
  {
    printf("ERROR: error accessing data file.\n");
    return -1;
  }

  rrdbTimeEpochSeconds oldestlasttouch = 0;

  /* Header */
  header = ( rrdbTouchHeader * ) addr;
  samplesPerSet = header->samplesPerSet;
  /* Make sure we are at the beggining of the sets */
  setHeader = ( rrdbTouchSet * )( addr + sizeof(rrdbTouchHeader) );

  for ( loopcount = 0; loopcount < header->sets; loopcount++ )
  {

    if ( 0 == oldestlasttouch && setHeader->lastTouch < oldestlasttouch )
    {
      oldestlasttouch = setHeader->lastTouch;
      oldestHeader = setHeader;
    }

    if ( 0 != strcmp(setHeader->path, path) )
    {
      goto continueloop;
    }

    if ( setHeader->period != period )
    {
      goto continueloop;
    }

    setHeader->lastTouch = time( NULL );

    retval = touchSet( header, setHeader, ( rrdbInt * ) ( setHeader + 1 ) );
    munmap( ( char * ) addr, sb.st_size );
    return retval;

    continueloop:

    ptr = (char *)setHeader;
    ptr += sizeof( rrdbTouchSet ) + ( header->samplesPerSet * sizeof( rrdbInt ) );
    setHeader = ( rrdbTouchSet * ) ptr;
  }

  /* If we get here, then we have not found one */
  if ( header->sets >= maxsets && NULL != oldestHeader )
  {
    /* in this condition we overwrite the oldest */
    setHeader = oldestHeader;
    setdata = ( rrdbInt * ) ( setHeader + 1 );
    memset( (void *) setdata, 0, samplesPerSet * sizeof(rrdbInt) );
  }
  else
  {
    /* Add a new one(s) */
    header->sets++;

    if ( fstat( pfd, &sb ) == -1 ) {/* To obtain file size */
      printf( "ERROR: failed to stat file \n" );
      return -1;
    }

    munmap( ( char * ) addr, sb.st_size );
    posix_fallocate( pfd, sb.st_size, sizeof( rrdbTouchSet ) + ( sizeof( rrdbInt ) * samplesPerSet ) );

    if ( fstat( pfd, &sb ) == -1 ) { /* To obtain file size */
      printf( "ERROR: failed to stat file\n" );
      return -1;
    }

    addr = mmap(NULL, sb.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, pfd, 0);
    if (addr == MAP_FAILED) {
      printf("ERROR: error accessing data file.\n");
      return -1;
    }

    setHeader = ( rrdbTouchSet * ) ( addr + sb.st_size - ( sizeof( rrdbTouchSet ) + ( sizeof( rrdbInt ) * samplesPerSet ) ) );
    setdata = ( rrdbInt * ) ( setHeader + 1 );
  }

  setHeader->lastTouch = time( NULL );
  setHeader->period = period;
  memset( setHeader->path, 0, sizeof( setHeader->path ) );
  strcpy( setHeader->path, path );

  int nowindesx = ( setHeader->lastTouch / getTimePerSample( period ) ) % samplesPerSet;
  setdata[ nowindesx ] = 1;

  munmap( ( char * ) addr, sb.st_size );

  return 1;
}

/************************************************************************************
 * Function: touchRRDBFile
 *
 * Purpose: This is a slightly different format of file than our initial RRDB file.
 * This format is to keep count of hits on a certain point. For example, with use in
 * babblevoice a path is the path through an auto attendant menu. For example,
 * a user dials 1 then 2 to navigate through a menu. The jumps in the extensions jump
 * through to extensions mainintro and then sales. The path then becomes mainintro/sales.
 * So we record a hit on the full path and then each sub path. Because we are slightly
 * more dynamic, i.e. the paths may vary - we need teh ability to remove old paths which
 * are no longer used (i.e. the oldest one) if we need a new one.
 *
 * Written: 7th March 2017 By: Nick Knight
 ************************************************************************************/
int touchRRDBFile(char *filename, char *path, char * period, unsigned int maxsets, unsigned int sampleCount)
{
  int pfd;
  unsigned int iperiod = 0;
  char *pathitem, *perioditem;
  char *pathitem_save_ptr, *perioditem_save_ptr;
  char *addr, *ptr;
  char periodcopy[MAXVALUESTRING];
  unsigned int i;
  unsigned int setsize = 0;
  off_t truncateby = 0;
  time_t now;
  rrdbTouchHeader *headerData;
  rrdbTouchSet *touchSet, *src;
  struct stat sb;

  if ( 0 == maxsets ) {
    maxsets = TOUCHMAXDEFAULTSETS;
  }

  if ( 0 == sampleCount ) {
    sampleCount = TOUCHDEFAULTSAMPLECOUNT;
  }

  if ((pfd = open(filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH )) == -1) {
    /* failure - should only ouput one error - perhaps need to put this somewhere else?*/
    int errsv = errno;
    printf("ERROR: failed to open %s for O_RDWR (%s)\n", filename, strerror(errsv));
    return -1;
  }

  if( 0 != lockf(pfd, F_LOCK, 1) ) {
    close(pfd);
    printf("ERROR: failed to acquire lock for file %s\n", filename);
    return -1;
  }

  if ( fstat( pfd, &sb ) == -1 ) { /* To obtain file size */
    lseek(pfd, 0, SEEK_SET);
    if( 0 != lockf(pfd, F_ULOCK, 1) ) {
      fprintf( stderr, "Failed to unlock file\n" );
    }
    close(pfd);
    printf("ERROR: Couldn't stat RRDB file(1)\n");
    return -1;
  }

  if ( 0 == sb.st_size ) {
    posix_fallocate( pfd, 0, sizeof(rrdbTouchHeader) );

    if ( fstat( pfd, &sb ) == -1 ) { /* To obtain file size */
      lseek(pfd, 0, SEEK_SET);
      if( 0 != lockf(pfd, F_ULOCK, 1) ) {
        fprintf( stderr, "Failed to unlock file\n" );
      }
      close(pfd);
      printf("ERROR: Couldn't stat RRDB file(2)\n");
      return -1;
    }

    headerData = ( rrdbTouchHeader * ) mmap(NULL, sizeof(rrdbTouchHeader), PROT_WRITE | PROT_READ, MAP_SHARED, pfd, 0);
    if (headerData == MAP_FAILED) {
      printf("ERROR: Failed to read RRDB file header\n");
      return -1;
    }

    /* In this version of RRDB file we create dynamically. */
    headerData->fileVersion = RRDBTOUCHV2;
    headerData->sets = 0;
    headerData->samplesPerSet = sampleCount;

    munmap( ( char * ) headerData, sizeof(rrdbTouchHeader) );
  }

  if ( RRDBTOUCHV2 != getFileVersion( pfd ) ) {
    printf("ERROR: Bad format for RRDB touch file\n");
    /* Unlock and close. */
    lseek(pfd, 0, SEEK_SET);
    if( 0 != lockf(pfd, F_ULOCK, 1) ) {
      fprintf( stderr, "failed to release lock for file\n" );
    }

    close(pfd);
    return -1;
  }

  if ( 0 == strlen( period ) ) {
    period = "d";
  }

  pathitem_save_ptr = NULL;
  // We only use path once, so it doesn't matter that strtok_r overwrites it
  pathitem = strtok_r( path, "/", &pathitem_save_ptr );
  while( NULL != pathitem ) {
    perioditem_save_ptr = NULL;
    strcpy( periodcopy, period );
    perioditem = strtok_r( periodcopy, ",", &perioditem_save_ptr );

    while( NULL != perioditem ) {
      if ( 0 == strcmp( perioditem, "FIVEMINUTE" ) ) {
        iperiod = FIVEMINUTE;
      } else if ( 0 == strcmp( perioditem, "ONEHOUR" ) ) {
        iperiod = ONEHOUR;
      } else if ( 0 == strcmp( perioditem, "SIXHOUR" ) ) {
        iperiod = SIXHOUR;
      } else if ( 0 == strcmp( perioditem, "TWELVEHOUR" ) ) {
        iperiod = TWELVEHOUR;
      } else if ( 0 == strcmp( perioditem, "ONEDAY" ) ) {
        iperiod = ONEDAY;
      } else {
        iperiod = ONEHOUR;
      }

      findTouchSet(pfd, pathitem, iperiod, maxsets);
      perioditem = strtok_r( NULL, ",", &perioditem_save_ptr );
    }
    pathitem = strtok_r( NULL, "/", &pathitem_save_ptr );
  }

  /* Remove any sets which haven't been touched for longer than the set size */
  if ( fstat( pfd, &sb ) == -1 ) { /* To obtain file size */
    lseek(pfd, 0, SEEK_SET);
    if( 0 != lockf(pfd, F_ULOCK, 1) ) {
      fprintf( stderr, "failed to release lock for file\n" );
    }

    close(pfd);
    printf("ERROR: Failed to stat RRDB file (3)\n");
    return -1;
  }

  addr = mmap(NULL, sb.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, pfd, 0);
  headerData = ( rrdbTouchHeader * ) addr;
  ptr = addr + sizeof( rrdbTouchHeader );
  now = time( NULL );
  setsize = sizeof( rrdbTouchSet ) + ( headerData->samplesPerSet * sizeof( rrdbInt ) );

  for( i = 0; i < headerData->sets; i++ ) {
    touchSet = ( rrdbTouchSet * ) ( ptr + ( setsize * i ) );
    if ( touchSet->lastTouch < ( now - ( getTimePerSample( touchSet->period ) * headerData->samplesPerSet ) ) ) {
      /* We need to remove */
      truncateby++;
      headerData->sets--;

      /* Copy the last one to this one (if not the last one) */
      if ( i == headerData->sets ) break;

      src = ( rrdbTouchSet * )
            ( addr + sizeof( rrdbTouchHeader ) +
            ( headerData->sets * setsize ) );

      memcpy( touchSet, src, setsize );

      /* Start again */
      i = 0;
    }
  }

  if ( truncateby > 0 && -1 == ftruncate( pfd, sb.st_size - ( setsize * truncateby ) ) ) {
    fprintf( stderr, "Failed to truncate file\n" );
  }

  munmap( addr, sb.st_size );

  /* Unlock and close. */
  lseek(pfd, 0, SEEK_SET);
  if( 0 != lockf(pfd, F_ULOCK, 1) ) {
    fprintf( stderr, "Failed to unlock file\n" );
  }

  close(pfd);

  return 0;
}


/************************************************************************************
 * Function: runCommand
 *
 * Purpose: runs the command
 *
 * Written: 10th March 2013 By: Nick Knight
 ************************************************************************************/
int runCommand(char *filename, RRDBCommand ourCommand, unsigned int sampleCount, unsigned int setCount, char *values, char *xformations, char * cperiod)
{
    rrdbFile ourFile;
    int pfd;
    int retval = 1;

    switch(ourCommand)
    {
        case CREATE:
            if ( 0 >= sampleCount ) {
                printf("ERROR: sample count too small, must be more than zero.\n");
                return -1;
            }

            if ( -1 == ( pfd = initRRDBFile(filename, setCount, sampleCount, xformations)) ) {
                printf("ERROR: writing db file error");
                return -1;
            }
            lseek(pfd, 0, SEEK_SET);
            if( 0 != lockf(pfd, F_ULOCK, 1) ) {
              fprintf( stderr, "Failed to unlock file\n" );
            }
            close(pfd);
            break;

        case FETCH:
            if( ( pfd = open(filename, O_RDWR ) ) == -1) {
              printf("ERROR: failed to read rrdb file '%s'\n", filename);
              return -1;
            }

            /* Lock */
            if( 0 != lockf(pfd, F_LOCK, 1) ) {
              printf("ERROR: failed to acquire lock read for rrdb file '%s'\n", filename);
              return -1;
            }

            switch( getFileVersion( pfd ) ) {
              case RRDBV1:
                if( -1 == readRRDBFile(pfd, &ourFile) ) goto getoutfetch;

                if ( 0 != strlen(xformations) ) {
                  if ( -1 == printRRDBFileXform(&ourFile, atoi(xformations))) {
                    retval = -1;
                    goto getoutfetch;
                  }
                } else {
                  if ( -1 == printRRDBFile(&ourFile)) {
                    retval = -1;
                    goto getoutfetch;
                  }
                }

                freeRRDBFile(&ourFile);
                break;
              case RRDBTOUCHV2:
                printRRDBTouchFile( pfd, xformations, cperiod );
                break;
              default:
                printf("ERROR: Unknown file format\n");
                retval = -1;
                goto getoutfetch;
                break;
            }
getoutfetch:
            lseek(pfd, 0, SEEK_SET);
            if( 0 != lockf(pfd, F_ULOCK, 1) ) {
              fprintf( stderr, "Failed to unlock file\n" );
            }
            close(pfd);

            break;

        case UPDATE:
            /* we should be given a value for each set we have */
            if ( -1 ==  updateRRDBFile(filename, &values[0])) return -1;
            break;

        case MODIFY:
            if ( -1 ==  modifyRRDBFile(filename, values, xformations)) return -1;
            break;

        case INFO:
            if ( -1 == printRRDBFileInfo(filename) ) return -1;
            break;

        case TOUCH:
            touchRRDBFile(filename, xformations, cperiod, setCount, sampleCount);
            break;

        case PIPE:
            break;
    }

    return retval;
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
int waitForInput(char *dir) {

  char c;
  int ic;
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

  char period[MAXCOMMANDLENGTH];

  char fulldirname[PATH_MAX + NAME_MAX];

  command[0] = 0;
  while( 1 ) {
    ic = fgetc( stdin );
    if( EOF == ic ) return -1;
    c = ic;
    if ( c == '\n' ) break;
    if ( c == '\r' ) continue;

    command[i] = c;
    command[i+1] = 0;

    if( i > ( MAXCOMMANDLENGTH - 5 ) ) {
      printf("ERROR: command too long\n");
      return -1;
    }

    i++;
  }

  if ( 0 == strlen(command)) return -1;

  /* command */
  result = strtok( command, delims );
  if ( 0 == strcmp("create", result) ) {
      ourCommand = CREATE;
  } else if ( 0 == strcmp("update", result) ) {
      ourCommand = UPDATE;
  } else if ( 0 == strcmp("fetch", result) ) {
      ourCommand = FETCH;
  } else if ( 0 == strcmp("info", result) ) {
      ourCommand = INFO;
  } else if ( 0 == strcmp("touch", result) ) {
    ourCommand = TOUCH;
  } else {
    /* we must have a command */
    printf("ERROR: no valid command so quiting\n");
    return -1;
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
  if ( NULL != result ) {
    if ( CREATE == ourCommand || TOUCH == ourCommand ) {
      setCount = atoi(result);
    } else if ( FETCH == ourCommand ) {
      if ( strlen(result) > MAXVALUESTRING ) {
        printf("ERROR: Length of xformations string too long\n");
        return -1;
      }
      strcpy( &xformations[0], result );
    } else {
      if ( strlen(result) > MAXVALUESTRING ) {
        printf("ERROR: Length of value string too long\n");
        return -1;
      }
      strcpy( &values[0], result );
    }
  }

  /* samplecount */
  result = strtok( NULL, delims );
  if ( NULL != result ) {
    /* Just in case this is a v2 touch. */
    strcpy( &period[0], result );

    /* or not */
    sampleCount = atoi(result);
  }

  if ( CREATE == ourCommand || TOUCH == ourCommand ) {
    result = strtok( NULL, delims );
    if ( NULL != result ) {
      if (strlen(result) > MAXVALUESTRING) {
        printf("ERROR: Length of xformation string too long\n");
        exit(1);

      }
      strcpy( &xformations[0], result );
    }
  }

  if ( TOUCH == ourCommand ) {
    result = strtok( NULL, delims );
    strcpy( &period[0], result );
  }

  if ( -1 == runCommand(fulldirname, ourCommand, sampleCount, setCount, values, xformations, period) ) {
    return -1;
  }

  printf("OK\n");
  return 1;
}

/************************************************************************************
 * Function: sigHandler
 *
 * Purpose: Handle UNIX signals.
 *
 * Written: 13th March 2013 By: Nick Knight
 ************************************************************************************/
static void sigHandler(int signo) {
  if (signo == SIGINT) {
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
int main(int argc, char **argv) {
  char c;
  RRDBCommand ourCommand;

  unsigned int setCount = 0;
  unsigned int sampleCount = 0;

  char dir[PATH_MAX];
  char fulldirname[PATH_MAX + NAME_MAX];

  char filename[NAME_MAX];
  char period[NAME_MAX];
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
      {"touchpath",   1, 0, 7 },
      {"period",      1, 0, 8 },
      {0,             0, 0, 0 }
  };

  int option_index = 0;

  setvbuf(stdin, NULL, _IOLBF, 0);
  setvbuf(stdout, NULL, _IOLBF, 0);

  memset(&dir[0], 0, PATH_MAX);
  memset(&filename[0], 0, NAME_MAX);
  memset(&values[0], 0, MAXVALUESTRING);

  if (signal(SIGINT, sigHandler) == SIG_ERR) {
      printf("ERROR: can't catch SIGINT\n");
      exit(1);
  }

  /* default */
  ourCommand = PIPE;

  while ((c = getopt_long(argc, argv, "",
                            long_options, &option_index)) != -1) {
    switch (c) {
      case 0:
        if ( 0 == strcmp("-", optarg) ) {
          ourCommand = PIPE;
        } else if ( 0 == strcmp("create", optarg) ) {
          ourCommand = CREATE;
        } else if ( 0 == strcmp("update", optarg) ) {
          ourCommand = UPDATE;
        } else if ( 0 == strcmp("fetch", optarg) ) {
          ourCommand = FETCH;
        } else if ( 0 == strcmp("info", optarg) ) {
          ourCommand = INFO;
        } else if ( 0 == strcmp("touch", optarg) ) {
          ourCommand = TOUCH;
        } else if ( 0 == strcmp("modify", optarg) ) {
          ourCommand = MODIFY;
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
        if ( strlen(optarg) > PATH_MAX ) {
          printf("ERROR: Length of path too long\n");
          exit(1);
        }
        strcpy( &dir[0], optarg );

        break;

      case 4:
        /* filename */
        if ( strlen(optarg) > NAME_MAX ) {
          printf("ERROR: Length of filename too long\n");
          exit(1);
        }
        strcpy( &filename[0], optarg );
  break;

      case 5:
        /* values */
        if ( strlen(optarg) > MAXVALUESTRING ) {
          printf("ERROR: Length of value string too long\n");
          exit(1);
        }

        strcpy( &values[0], optarg );
        break;

      case 6:
        /* xformations */
        if ( strlen(optarg) > MAXVALUESTRING ) {
          printf("ERROR: Length of xform string too long\n");
          exit(1);
        }
        strcpy( &xformations[0], optarg );
        break;

      case 7:
        /* touchpath */
        if ( strlen(optarg) > MAXVALUESTRING ) {
          printf("ERROR: Length of touchpath string too long\n");
          exit(1);
        }
        strcpy( &xformations[0], optarg );
        break;
      case 8:
        /* period */
        strcpy( &period[0], optarg );
        break;

      default:
        /* Unknown option */
        exit(1);
    }
  }

  if ( PIPE == ourCommand ) {
      while(-1 != waitForInput(dir));
  } else {
    strcpy(&fulldirname[0], &dir[0]);
    pathlength = strlen(dir);
    fulldirname[pathlength] = '/';
    fulldirname[pathlength+1] = 0;
    pathlength++;

    strcpy(&fulldirname[pathlength], &filename[0]);

    runCommand(fulldirname, ourCommand, sampleCount, setCount, values, xformations, period);

  }

  /* mainly to keep users of valgrind happy as to while 3 file descriptors are still open */
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  return 0;
}
