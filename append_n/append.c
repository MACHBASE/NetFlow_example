#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <machbase_sqlcli.h>

#define MACHBASE_PORT_NO	5656
#define ERROR_CHECK_COUNT	100

#define RC_SUCCESS          0
#define RC_FAILURE          -1

#define UNUSED(aVar) do { (void)(aVar); } while(0)

#define CHECK_STMT_RESULT(aRC, aSTMT, aMsg)     \
    if( sRC != SQL_SUCCESS )                    \
    {                                           \
        printError(gEnv, gCon, aSTMT, aMsg);    \
        goto error;                             \
    }                                        


SQLHENV 	gEnv;
SQLHDBC 	gCon;


static int gLimit = 0;
static int gInterval = 0;


void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
time_t getTimeStamp();
int connectDB();
void disconnectDB();
int executeDirectSQL(const char *aSQL, int aErrIgnore);
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt,char *pDate);
SQLBIGINT appendClose(SQLHSTMT aStmt);

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

void appendDumpError(SQLHSTMT    aStmt,
                 SQLINTEGER  aErrorCode,
                 SQLPOINTER  aErrorMessage,
                 SQLLEN      aErrorBufLen,
                 SQLPOINTER  aRowBuf,
                 SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}

time_t getTimeStamp()
{
    struct timeval sTimeVal;

    gettimeofday(&sTimeVal, NULL);

    return (sTimeVal.tv_sec*1000000 + sTimeVal.tv_usec);
}


int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS ) 
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS ) 
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", MACHBASE_PORT_NO);

    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int executeDirectSQL(const char *aSQL, int aErrIgnore)
{
    SQLHSTMT sStmt = SQL_NULL_HSTMT;

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS )
    {
        if( aErrIgnore == 0 ) 
        {
            printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
            return RC_FAILURE;
        }
    }

    if( SQLExecDirect(sStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {

        if( aErrIgnore == 0 )
        {
            printError(gEnv, gCon, sStmt, "SQLExecDirect Error");

            SQLFreeStmt(sStmt,SQL_DROP);
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        if (aErrIgnore == 0) 
        {
            printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }
    sStmt = SQL_NULL_HSTMT;

    return RC_SUCCESS; 
}

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = "NETFLOW_DATA";

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int appendData(SQLHSTMT aStmt,char *pDate)
{
    const char      *sFileName = "data.csv";
    FILE            *sFp       = NULL;
    char             sBuf[1024];
    char            *sfullToken;
    char            sToken0[30];
    char            sToken1[30];
    char            sToken2[10];
    char            sToken3[16];
    char            sToken4[10];
    char            sToken5[10];
    char            sToken6[16];
    char            sToken7[10];
    char            sToken8[50];
    char            sToken9[50];
    char            sToken10[50];
    char            sToken11[50];
    char            sToken12[50];
    char            sToken13[50];
    char            sToken14[128];
    SQLBIGINT        sCount=0;
    size_t           sTokenSize =0;
    size_t           sTokenEnd =0;


    SQL_APPEND_PARAM sParam[15];

    sFp = fopen(sFileName, "r");
    if( !sFp )
    {
        printf("file open error-%s\n", sFileName);
        return RC_FAILURE;
    }

    printf("append data start\n");

    memset(sBuf, 0, sizeof(sBuf));
    memset(sParam, 0, sizeof(sParam));

    while( fgets(sBuf, 1024, sFp ) != NULL )
    {
        if( strlen(sBuf) < 1)
        {
            break;
        }

        sfullToken = sBuf;
        sTokenSize = 0;

        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken0, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken1, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken2, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken3, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken4, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken5, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken6, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken7, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken8, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken9, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken10, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken11, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken12, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken13, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;
        
        sTokenEnd = strcspn(sfullToken + sTokenSize, ",");
        snprintf(sToken14, sTokenEnd+1, "%.*s\n", sTokenEnd+1, sfullToken + sTokenSize);
        sTokenSize += (sfullToken[sTokenSize + sTokenEnd] != NULL) ? sTokenEnd + 1 : sTokenEnd;

        sParam[0].mDateTime.mTime = getTimeStamp() * 1000;
        
        sParam[1].mDouble = atof(sToken1); //double dur
        sParam[2].mVar.mLength = strlen(sToken2);
        sParam[2].mVar.mData = sToken2;
        sParam[5].mVar.mLength = strlen(sToken5);
        sParam[5].mVar.mData = sToken5;
        sParam[8].mVar.mLength = strlen(sToken8);
        sParam[8].mVar.mData = sToken8;
        
        sParam[3].mIP.mLength = SQL_APPEND_IP_STRING;
        sParam[3].mIP.mAddrString = sToken3;
        sParam[6].mIP.mLength = SQL_APPEND_IP_STRING;
        sParam[6].mIP.mAddrString = sToken6;
       
        sParam[4].mInteger = atoi(sToken4); //int
        sParam[7].mInteger = atoi(sToken7); //int
        sParam[9].mInteger = atoi(sToken9); //int
        sParam[10].mInteger = atoi(sToken10); //int
        sParam[11].mInteger = atoi(sToken11); //int
        sParam[12].mInteger = atoi(sToken12); //int
        sParam[13].mInteger = atoi(sToken13); //int
        
        sParam[14].mVar.mLength = strlen(sToken14);
        sParam[14].mVar.mData = sToken14;
        
        //sToken = strtok(NULL, ",");
        

        if( SQLAppendDataV2(aStmt, sParam) != SQL_SUCCESS )
        {
            printError(gEnv, gCon, aStmt, "SQLAppendData Error");
            return RC_FAILURE;
        }

        if ( ((sCount++) % gLimit) == 0)
        {
            fprintf(stdout, "%d   ", sCount);
            fflush(stdout);
            usleep(gInterval * 1000);
        }

        if( ((sCount) % 100) == 0 )
        {
            if( SQLAppendFlush( aStmt ) != SQL_SUCCESS )
            {
                printError(gEnv, gCon, aStmt, "SQLAppendFlush Error");
            }
        }
    }

    printf("\nappend data end\n");

    fclose(sFp);

    return RC_SUCCESS;
}

SQLBIGINT appendClose(SQLHSTMT aStmt)
{
    SQLBIGINT sSuccessCount = 0;
    SQLBIGINT  sFailureCount = 0;

    if( SQLAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int main(int argc, char *argv[])
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;
    char        SaveDate[11]={0,};
    SQLBIGINT   sCount=0;
    int         i=0;
    time_t      sStartTime, sEndTime;

    if ( argc == 3 )
    {
        gLimit = atoi(argv[1]);
        gInterval = atoi(argv[2]);
    }
    else
    {
        printf("Input correct arg\n");
        exit(1);
    }

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }


    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }
    for(i=1; i < 1002; i++)
    {
        snprintf(SaveDate,11,"2012/06/%02d",i);
        sStartTime = getTimeStamp();
        appendData(sStmt,SaveDate);
        sEndTime = getTimeStamp();
    }

    sCount = appendClose(sStmt);
    if( sCount >= 0 )
    {
        printf("appendClose success\n");
        printf("timegap = %ld microseconds for %lld records\n", sEndTime - sStartTime, sCount);
        printf("%.2f records/second\n",  ((double)sCount/(double)(sEndTime - sStartTime))*1000000);
    }
    else
    {
        printf("appendClose failure\n");
    }


    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

    disconnectDB();

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }

    return RC_FAILURE;
}
