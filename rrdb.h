


#ifndef RRDB_H
#define RRDB_H

#define MAXNUMSETS 20
#define MAXNUMXFORMPERSET 5
#define MAXVALUESTRING 600
#define MAXCOMMANDLENGTH 600


#define TRUE 1
#define	FALSE 0

#define UNUSED(x) (void)(x)

/* data sizes */
typedef long double rrdbNumber;
typedef char rrdbValid;
typedef time_t rrdbTimeEpochSeconds;
typedef unsigned short rrdbTimemSeconds;


typedef enum {PIPE, CREATE, UPDATE, FETCH, INFO} RRDBCommand;

/*
 * File structure for our db file
 */
typedef struct rrdbHeader
{
    /*
     Just check the file looks sencible.
     */
	int fileVersion;
    /*
     Where abouts in our RRD circle are we?
     */
	unsigned int windowPosition;
    /* 
     Set count can be zero or more. 0 would be event only
     (i.e. a record of the time). But also we can have multiple 
     values for each reading.
     */
    unsigned int setCount;
    
    /* the number of samples in a set */
    unsigned int sampleCount;
    
} rrdbHeader;


typedef struct rrdbTimePoint
{
    /* UNIX Time (EPOCH) */
	rrdbTimeEpochSeconds time;
    /* mS after the timeSinceEpoch */
	rrdbTimemSeconds uSecs;

    /* is this entry valid */
    rrdbValid valid;
} rrdbTimePoint;

typedef enum {FIVEMINUTE = 0, ONEHOUR = 1, SIXHOUR = 2, TWELVEHOUR = 3, ONEDAY = 4} RRDBTimePeriods;
typedef enum {RRDBMAX = 0, RRDBMIN = 1, RRDBCOUNT = 2, RRDBMEAN = 3, RRDBSUM = 4} RRDBCalculation;

typedef struct rrdbXformsHeader
{
    unsigned int xformCount;
    
} rrdbXformsHeader;

typedef struct rrdbXformHeader
{
    unsigned int period;
    unsigned int calc;
    unsigned int setIndex;
    /* each xform has to maintain its own position as it will differ as they all have differing time periods */
    unsigned int windowPosition;
    
} rrdbXformHeader;


typedef struct rrdbFile
{
	rrdbHeader header;
    /* The time values for each point */
	rrdbTimePoint *times;
    
    /* array of pointers */
    rrdbNumber *sets[MAXNUMSETS];
    
    
    rrdbXformsHeader xformheader;
    
    /* array of pointers to our xformations */
    rrdbXformHeader xforms[MAXNUMSETS * MAXNUMXFORMPERSET];
    
    rrdbTimePoint *xformtimes[MAXNUMSETS * MAXNUMXFORMPERSET];
    rrdbNumber *xformdata[MAXNUMSETS * MAXNUMXFORMPERSET];
    
} rrdbFile;


int initRRDBFile(char *filename, unsigned int setCount, unsigned int sampleCount , char *xformations);
int readRRDBFile(char *filename, rrdbFile *fileData);
int writeRRDBFile(int pfd, rrdbFile *fileData);
int updateRRDBFile(char *filename, char* vals);
int freeRRDBFile(rrdbFile *fileData);
int printRRDBFile(rrdbFile *fileData);
int printRRDBFileInfo(char *filename);
int printRRDBFileXform(rrdbFile *fileData, unsigned int index);
int waitForInput(char *dir);
int runCommand(char *filename, RRDBCommand ourCommand, unsigned int sampleCount, unsigned int setCount, char *values, char *xformations);

/* xformations */
rrdbNumber calcRRDBCount(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex);
rrdbNumber calcRRDBSum(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex);
rrdbNumber calcRRDBMean(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex);
rrdbNumber calcRRDBMin(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex);
rrdbNumber calcRRDBMax(struct timeval* start, struct timeval *end, rrdbFile *fileData, unsigned int setIndex);

#endif /* RRDB_H */


