/*
 *  SQLHelpers.m
 *  iPPromet, taken from GlideShow
 *
 *  Created by Igor Delovski on 02.07.2011.
 *  Copyright 2011 Igor Delovski, Calyx d.o.o.. All rights reserved.
 *
 */


#import "SQLHelpers.h"

#import "FileHelpers.h"
#import "DataUtils.h"

static SQLFldDescription  internalSQLFields[] = {
    { @"id",    kNUMBER_Class, SQLITE_INTEGER, kFldDescFlagSearchableKey | kFldDescFlagNotNullKey, 0 },

    { nil, 0, }
};


@implementation SQLHelper

#pragma mark -

@synthesize  fileName;

#pragma mark -

// .........................................................................................................

+ (int)createNewDatabase:(NSString *)filePath creatingTableWithSqlString:(NSString *)sqlStr errString:(NSString **)retErrStr
{
   int          returnCode;
   const char  *cSqlStr = [sqlStr UTF8String];
   const char  *tmpMsg;
   char        *errMsg = NULL;
   sqlite3     *tmpDbHandle;
   
   returnCode = sqlite3_open ([filePath UTF8String], &tmpDbHandle);
   
#ifdef kSQLITE_LOG_
   NSLog (@"createNewDatabase:creatingTableWithSqlString:errString: - code: %d str: %@", returnCode, sqlStr);
#endif

   if (returnCode != SQLITE_OK)  {
      tmpMsg = sqlite3_errmsg (tmpDbHandle);
      if (retErrStr)
         *retErrStr = [NSString stringWithUTF8String:tmpMsg];
      NSLog (@"Error sqlite3_open() - %s", tmpMsg);
      if (tmpDbHandle)  {
         sqlite3_close (tmpDbHandle);
         tmpDbHandle = NULL;
      }
      return (returnCode);
   }
   
   returnCode = sqlite3_exec (tmpDbHandle, cSqlStr, NULL, NULL, &errMsg);
   
   if ((returnCode != SQLITE_OK) && errMsg)  {
      NSLog (@"Error sqlite3_exec() - %s", errMsg);
      if (retErrStr)
         *retErrStr = [NSString stringWithUTF8String:errMsg];
      sqlite3_free (errMsg);
   }
   else  if (retErrStr)
      *retErrStr = nil;
   
   return (returnCode);
}

// .........................................................................................................

- (id)init
{
   // Dont call us (We'll call you)
   
   return (nil);
}

// .........................................................................................................

- (id)initWithFilePath:(NSString *)filePath
{
   int  returnCode;
   
   if (self = [super init])  {   
      returnCode = sqlite3_open ([filePath UTF8String], &dbHandleFore);
      
      if (returnCode != SQLITE_OK)
         NSLog (@"Error sqlite3_open() - %s", sqlite3_errmsg(dbHandleFore));
      else  {
         returnCode = sqlite3_open ([filePath UTF8String], &dbHandleBack);
         if (returnCode != SQLITE_OK)
            NSLog (@"Error sqlite3_open() - %s", sqlite3_errmsg(dbHandleBack));
      }
      
      if (returnCode != SQLITE_OK)  {
         if (dbHandleFore)  {
            sqlite3_close (dbHandleFore);
            dbHandleFore = NULL;
         }
         if (dbHandleBack)  {
            sqlite3_close (dbHandleBack);
            dbHandleBack = NULL;
         }
         [self release];
         self = nil;
      }
      else
         self.fileName = [filePath lastPathComponent];  // needs to retain it!
   }
   
   return (self);
}

// .........................................................................................................

- (id)initWithFileInDocumentsDirectory:(NSString *)aFileName
{
    NSString  *fullPath = [FileHelper pathInDocumentDirectory:aFileName];
   
   return ([self initWithFilePath:fullPath]);
}

// .........................................................................................................

- (id)initWithFileInCacheDirectory:(NSString *)aFileName
{
   NSString  *fullPath = [FileHelper pathInCacheDirectory:aFileName];
   
   return ([self initWithFilePath:fullPath]);
}

// .........................................................................................................

- (void)dealloc
{
   if (dbHandleFore)  {
      sqlite3_close (dbHandleFore);
      dbHandleFore = NULL;
   }
   if (dbHandleBack)  {
      sqlite3_close (dbHandleBack);
      dbHandleBack = NULL;
   }
   
   [fileName release];
   
   [super dealloc];
}

// .........................................................................................................

#pragma mark -

// .........................................................................................................

- (sqlite3 *)dbHandle
{
   if ([NSThread isMainThread])
      return (dbHandleFore);
   
   return (dbHandleBack);
}

// .........................................................................................................

#pragma mark -

// .........................................................................................................

- (NSString *)sqlStringWithString:(NSString *)origStr
{
   return ([origStr stringByReplacingOccurrencesOfString:@"'" withString:@"''"]);
}

// .........................................................................................................

- (NSString *)stringWithSqlString:(NSString *)sqlStr
{
   return ([sqlStr stringByReplacingOccurrencesOfString:@"''" withString:@"'"]);
}

// .........................................................................................................

// WTF - this ain't used anywhere

- (BOOL)shouldContinueLoopingWithReturnCode:(int)retCode andNumberOfRetries:(int)numberOfRetries
{
   int   maxRetries = kMaxBusyRetryCount;
   BOOL  retryFlag = YES;
   
   if ([NSThread isMainThread])
      maxRetries *= 3;
   
   if ((retCode == SQLITE_BUSY) || (retCode == SQLITE_LOCKED))  {
      if (numberOfRetries <= maxRetries)
         usleep (20);
      else
         retryFlag = NO;
   }
   else
      retryFlag = NO;
   
   return (retryFlag);
}

// .........................................................................................................

#pragma mark -

// .........................................................................................................

// Use for insert, update, delete
- (int)execSqlString:(NSString *)sqlString errString:(NSString **)retErrStr
{
   int          returnCode;
   char        *errMsg = NULL;
   const char  *utfString = [sqlString UTF8String];
   
   returnCode = sqlite3_exec (self.dbHandle, utfString, NULL, NULL, &errMsg);
   
   if ((returnCode != SQLITE_OK) && errMsg)  {
      NSLog (@"Error sqlite3_exec() - %s", errMsg);
      if (retErrStr)
         *retErrStr = [NSString stringWithUTF8String:errMsg];
      sqlite3_free (errMsg);
   }
   else  if (retErrStr)
      *retErrStr = nil;
   
   return (returnCode);
}

// .........................................................................................................

#pragma mark -

// .........................................................................................................

- (int)prepare:(NSString *)sqlString sqlStatement:(sqlite3_stmt **)retStatement
{
   int          returnCode;
   int          maxRetries = kMaxBusyRetryCount;
   int          numberOfRetries = 0;
   BOOL         retryFlag = YES;
   const char  *utfString = [sqlString UTF8String];
   
   sqlite3_stmt  *tmpStatement = NULL;
   
   if ([NSThread isMainThread])
      maxRetries *= 3;
   
   while (retryFlag)  {
      returnCode = sqlite3_prepare_v2 (self.dbHandle, utfString, -1, &tmpStatement, NULL);
      
      if ((returnCode == SQLITE_BUSY) || (returnCode == SQLITE_LOCKED))  {
         if (numberOfRetries++ <= maxRetries)
            usleep (20);
         else
            retryFlag = NO;
      }
      else
         retryFlag = NO;
   }
   
   if (returnCode != SQLITE_OK)  {
      NSLog (@"Error sqlite3_prepare() - %s", sqlite3_errmsg(self.dbHandle));
      [self finalizeSqlStatement:tmpStatement];
   }
   
   *retStatement = tmpStatement;
   
   return (returnCode);
}

// .........................................................................................................

- (int)stepWithSqlStatement:(sqlite3_stmt *)statement
{
   BOOL  retryFlag = TRUE;
   int   maxRetries = kMaxBusyRetryCount;
   int   numberOfRetries = 0;
   int   returnCode;
   
   if ([NSThread isMainThread])
      maxRetries *= 3;
   
   while (retryFlag)  {
      returnCode = sqlite3_step (statement);
      
      if ((returnCode == SQLITE_BUSY) || (returnCode == SQLITE_LOCKED))  {
         if (numberOfRetries++ <= maxRetries)
            usleep (20);
         else
            retryFlag = NO;
      }
      else
         retryFlag = NO;
   }
   
   if (returnCode != SQLITE_ROW && returnCode != SQLITE_DONE)
      NSLog (@"Error sqlite3_step() - [%d] - %s after %d retries.",
             returnCode, sqlite3_errmsg(self.dbHandle), numberOfRetries);
   
   return (returnCode);
}

// .........................................................................................................

- (int)finalizeSqlStatement:(sqlite3_stmt *)statement
{
   return (sqlite3_finalize(statement));
}

// .........................................................................................................

#pragma mark -

// .........................................................................................................

- (int)bindText:(const char *)txt withSqlStatement:(sqlite3_stmt *)statement forParamNumber:(NSInteger)paramNum asTransient:(BOOL)asTrans
{
   int  returnCode;
   
   if (!txt)
      returnCode = sqlite3_bind_null (statement, paramNum);
   else
      returnCode = sqlite3_bind_text (statement, paramNum, (const char *)txt, -1, asTrans ? SQLITE_TRANSIENT : SQLITE_STATIC);
   
   return (returnCode);
}

// .........................................................................................................

- (int)bindInt:(int)intVal withSqlStatement:(sqlite3_stmt *)statement forParamNumber:(NSInteger)paramNum
{
   int  returnCode;
   
   returnCode = sqlite3_bind_int (statement, paramNum, intVal);
   
   return (returnCode);
}

// .........................................................................................................

- (int)bindBlob:(const void *)ptr withSqlStatement:(sqlite3_stmt *)statement forParamNumber:(NSInteger)paramNum ofLength:(NSInteger)length asTransient:(BOOL)asTrans
{
   int  returnCode;
   
   if (!ptr)
      returnCode = sqlite3_bind_null (statement, paramNum);
   else
      returnCode = sqlite3_bind_blob (statement, paramNum, (const char *)ptr, length, asTrans ? SQLITE_TRANSIENT : SQLITE_STATIC);
   
   return (returnCode);
}

// .........................................................................................................

#pragma mark -

// .........................................................................................................

- (const unsigned char *)textWithSqlStatement:(sqlite3_stmt *)statement forColumnIndex:(NSInteger)idx
{
   return (sqlite3_column_text(statement, idx));
}

// .........................................................................................................

- (int)intWithSqlStatement:(sqlite3_stmt *)statement forColumnIndex:(NSInteger)idx
{
   return (sqlite3_column_int(statement, idx));
}

// .........................................................................................................

- (const void *)blobWithSqlStatement:(sqlite3_stmt *)statement forColumnIndex:(NSInteger)idx andLength:(NSInteger *)retLength
{
   const void  *retBytes = sqlite3_column_blob (statement, idx);
   
   if (retLength)  {
      if (!retBytes)
         *retLength = 0;
      else
         *retLength = sqlite3_column_bytes (statement, idx);  // size in bytes
   }
   
   return (retBytes);
}

// .........................................................................................................

#pragma mark -
#pragma mark SQL Strings Forming
#pragma mark -

// .........................................................................................................

+ (NSString *)creationTypeStringForFldDescription:(SQLFldDescription *)fldDesc
{
   NSString  *retStr = nil;
   
   switch (fldDesc->sqlType)  {
      case  SQLITE_INTEGER:  retStr = @"INTEGER";  break;
      case  SQLITE_FLOAT:    retStr = @"FLOAT";    break;
      case  SQLITE_BLOB:     retStr = @"BLOB";     break;
      case  SQLITE_NULL:     retStr = @"NULL";     break;
         
      default:               retStr = @"TEXT";     break;
   }
   
   if (fldDesc->fldFlags & kFldDescFlagUniqueKey)
      return ([NSString stringWithFormat:@"%@ UNIQUE", retStr]);
   
   return (retStr);
}

// .........................................................................................................

+ (NSString *)sqlWhereClauseStringWithSearchFields:(NSArray *)fldNamesToMatchOrNil
                             withSearchFieldValues:(NSArray *)fldValuesToMatchOrNil        // just to check for nulls
                              usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                                returningUsedCount:(int *)boundCnt;
{
   NSMutableString  *retStr = nil;
   NSString         *paramPart;
   int               i, pix, aix, qMarksCnt = 0;
   id                fldObj;
   
   if (!fldNamesToMatchOrNil && fldValuesToMatchOrNil)  {  // id only
      retStr = (NSMutableString *)@"id = ?";   // Never gonna give you up, Never gonna let you down, Never gonna run around and desert you
      qMarksCnt = 1;
   }
   else  {
      retStr = [NSMutableString string];
      for (pix=i=0; fldDescCArray[i].fldName; i++)  {
         if ((fldDescCArray[i].fldFlags & kFldDescFlagSearchableKey) &&
             ([fldNamesToMatchOrNil containsObject:fldDescCArray[i].fldName]))  {
            
            aix    = [fldNamesToMatchOrNil indexOfObject:fldDescCArray[i].fldName];
            fldObj = [fldValuesToMatchOrNil objectAtIndex:aix];
            
            if (fldObj == [NSNull null])
               paramPart = @"IS NULL";
            else  {
               paramPart = @"= ?";
               qMarksCnt++;
            }
            
            if (!pix)
               [retStr appendFormat:@"%@ %@", fldDescCArray[i].fldName, paramPart];
            else
               [retStr appendFormat:@" AND %@ %@", fldDescCArray[i].fldName, paramPart];
            pix++;
         }
      }
   }
   
   if (boundCnt)
      *boundCnt = qMarksCnt;
   
   return (retStr);
}

// .........................................................................................................

+ (NSString *)sqlCreationStringForTable:(NSString *)tableName
                    withFldDescriptions:(SQLFldDescription *)fldDescCArray
{
   // "CREATE TABLE %s (id integer primary key, name TEXT UNIQUE, title TEXT, image BLOB, thumb BLOB, comment TEXT);", kTableName);
   
   NSMutableString  *retStr = [NSMutableString stringWithFormat:@"CREATE TABLE %@ (id integer primary key", tableName];
   
   for (int i=0; fldDescCArray[i].fldName; i++)  {
      [retStr appendFormat:@", %@ %@", fldDescCArray[i].fldName, [self creationTypeStringForFldDescription:&fldDescCArray[i]]];
   }
   
   [retStr appendString:@");"];
   
   return (retStr);
}

// .........................................................................................................

+ (NSString *)sqlColumnAppendingStringForTable:(NSString *)tableName
                            withFldDescription:(SQLFldDescription *)oneFldDesc
{
   // "ALTER TABLE %s ADD COLUMN aColumnName TEXT;", kTableName);
   
   NSMutableString  *retStr = [NSMutableString stringWithFormat:@"ALTER TABLE %@ ADD COLUMN", tableName];
   
   [retStr appendFormat:@" %@ %@", oneFldDesc->fldName, [self creationTypeStringForFldDescription:oneFldDesc]];
   
   [retStr appendString:@";"];
   
   return (retStr);
}

// .........................................................................................................

+ (NSString *)sqlInsertionStringForTable:(NSString *)tableName
                     withFldDescriptions:(SQLFldDescription *)fldDescCArray
{
   // @"INSERT INTO %s (name, title, image, thumb, comment) values (?,?,?,?,?)", kTableName];
   
   NSMutableString  *retStr    = [NSMutableString stringWithFormat:@"INSERT INTO %@ (", tableName];
   NSMutableString  *qMarksStr = [NSMutableString stringWithFormat:@"VALUES ("];
   
   for (int i=0; fldDescCArray[i].fldName; i++)  {
      if (!i)  {
         [qMarksStr appendString:@"?"];
         [retStr appendFormat:@"%@", fldDescCArray[i].fldName];
      }
      else  {
         [retStr appendFormat:@", %@", fldDescCArray[i].fldName];
         [qMarksStr appendString:@", ?"];
      }
   }
   
   [retStr appendFormat:@") %@);", qMarksStr];
   
   return (retStr);
}

// .........................................................................................................

+ (NSString *)sqlUpdatingStringForTable:(NSString *)tableName
                       withSearchFields:(NSArray *)fldNamesToMatchOrNil
                  andSearchFieldValues:(NSArray *)fldValuesToMatchOrNil        // just to check for nulls
                   usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                   andOnlyFieldToUpdate:(NSString *)onlyFieldOrNil
                     returningUsedCount:(int *)boundCnt
{
    // @"UPDATE %s SET %s = ? WHERE name = ?", kTableName, thumbFlag ? "thumb" : "image"];
    
    int  pix, i, retCnt1, retCnt2;
    
    NSMutableString  *retStr = [NSMutableString stringWithFormat:@"UPDATE %@ SET ", tableName];
    
    for (pix=i=0; fldDescCArray[i].fldName; i++)  {
        if (onlyFieldOrNil && (![fldDescCArray[i].fldName isEqualToString:onlyFieldOrNil]))
            continue;
        
        if (!pix)
            [retStr appendFormat:@"%@ = ?", fldDescCArray[i].fldName];
        else
            [retStr appendFormat:@", %@ = ?", fldDescCArray[i].fldName];
        pix++;
    }
    
    retCnt1 = pix;
    
    [retStr appendString:@" WHERE "];
    
    if (fldNamesToMatchOrNil || (!fldNamesToMatchOrNil && fldValuesToMatchOrNil))  {
        
        [retStr appendString:[self sqlWhereClauseStringWithSearchFields:fldNamesToMatchOrNil
                                                  withSearchFieldValues:fldValuesToMatchOrNil        // just to check for nulls
                                                   usingFldDescriptions:fldDescCArray
                                                     returningUsedCount:&retCnt2]];
    }
    else  {
        // NULL values are not handled here!
        for (pix=i=0; fldDescCArray[i].fldName; i++)  {
            if (fldDescCArray[i].fldFlags & kFldDescFlagSearchableKey)  {
                if (!pix)
                    [retStr appendFormat:@"%@ = ?", fldDescCArray[i].fldName];
                else
                    [retStr appendFormat:@" AND %@ = ?", fldDescCArray[i].fldName];
                pix++;
            }
        }
        retCnt2 = pix;
    }
    
    if (boundCnt)
        *boundCnt = retCnt1 + retCnt2;  // pix starts at zero, so 3 means 3 items bound
    
    return (retStr);
}

// .........................................................................................................

// if fldNamesToMatchOrNil is nil, include all searchable fields into where clause

+ (NSString *)sqlDeletionStringForTable:(NSString *)tableName
                       withSearchFields:(NSArray *)fldNamesToMatchOrNil
                   andSearchFieldValues:(NSArray *)fldValuesToMatchOrNil        // just to check for nulls
                   usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                     returningUsedCount:(int *)boundCnt
{
   // @"DELETE FROM %s WHERE name = ?", kTableName]
   
   int  pix, i;
   
   NSMutableString  *retStr = [NSMutableString stringWithFormat:@"DELETE FROM %@ WHERE ", tableName];
   
   if (fldNamesToMatchOrNil || (!fldNamesToMatchOrNil && fldValuesToMatchOrNil))  {
      
      [retStr appendString:[self sqlWhereClauseStringWithSearchFields:fldNamesToMatchOrNil
                                                withSearchFieldValues:fldValuesToMatchOrNil        // just to check for nulls
                                                 usingFldDescriptions:fldDescCArray
                                                   returningUsedCount:&pix]];
   }
   else  {
      for (pix=i=0; fldDescCArray[i].fldName; i++)  {
         if (fldDescCArray[i].fldFlags & kFldDescFlagSearchableKey)  {
            if (!pix)
               [retStr appendFormat:@"%@ = ?", fldDescCArray[i].fldName];
            else
               [retStr appendFormat:@" AND %@ = ?", fldDescCArray[i].fldName];
            pix++;
         }
      }
   }
   
   if (boundCnt)
      *boundCnt = pix;  // pix starts at zero, so 3 means 3 items bound
   
   return (retStr);
}

// .........................................................................................................

// Pass selParams only or fldNamesToMatch and fldDescCArray together

+ (NSString *)sqlSelectionStringForTable:(NSString *)tableName
                        withSelectParams:(SQLSelectParams *)selParams
                      orJustSearchFields:(NSArray *)fldNamesToMatchOrNil
                   withSearchFieldValues:(NSArray *)fldValuesToMatchOrNil        // just to check for nulls
                    usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                    andOnlyFieldToSelect:(NSString *)onlyFieldOrNil
                      returningUsedCount:(int *)boundCnt;
{
   // [... @"SELECT %s FROM %s WHERE name = ?", "image", kTableName];
   // But there is also [... @"SELECT DISTINCT column FROM table"];
   
   // int               i, pix;
   NSMutableString  *retStr = nil;
   
   if (selParams && selParams.distinctFlag && onlyFieldOrNil)
      retStr = [NSMutableString stringWithFormat:@"SELECT DISTINCT %@ FROM %@", onlyFieldOrNil, tableName];
   else
      retStr = [NSMutableString stringWithFormat:@"SELECT %@ FROM %@ WHERE ", onlyFieldOrNil ? onlyFieldOrNil : @"*", tableName];
   
   if (fldNamesToMatchOrNil || (!fldNamesToMatchOrNil && fldValuesToMatchOrNil))  {
      
      // This covers both custom rows (fldNamesToMatchOrNil + fldValuesToMatchOrNil) and
      // search by rowid (only fldValuesToMatchOrNil)
      
      [retStr appendString:[self sqlWhereClauseStringWithSearchFields:fldNamesToMatchOrNil
                                                withSearchFieldValues:fldValuesToMatchOrNil        // just to check for nulls
                                                 usingFldDescriptions:fldDescCArray
                                                   returningUsedCount:boundCnt]];
   }
   else  if (selParams)  {
      if (!selParams.distinctFlag)  {
         [retStr appendString:selParams.whereClause];
         
         if (selParams.orderByClause)
            [retStr appendFormat:@" ORDER BY %@", selParams.orderByClause];
         if (selParams.limitCnt && selParams.offsetCnt)
            [retStr appendFormat:@" LIMIT %d OFFSET %d", selParams.limitCnt, selParams.offsetCnt];
         else  if (selParams.limitCnt)
            [retStr appendFormat:@" LIMIT %d", selParams.limitCnt];
      }
      
      if (boundCnt)
         *boundCnt = 0;
   }
   
   return (retStr);
}

#pragma mark -

- (int)bindSingleObject:(id)fldObj
         toSqlStatement:(sqlite3_stmt *)sqlStatement
            atBindIndex:(int)bindIndex  // one based
    usingFldDescription:(SQLFldDescription *)fldDesc
{
   int  returnCode;
   
   if (!fldObj)
      returnCode = sqlite3_bind_null(sqlStatement, bindIndex);
   
   else  if ([fldObj isKindOfClass:[NSString class]])  {
      returnCode = [self bindText:[[self sqlStringWithString:fldObj] UTF8String]
                 withSqlStatement:sqlStatement
                   forParamNumber:bindIndex
                      asTransient:YES];
   }
   else  if ([fldObj isKindOfClass:[NSData class]])  {
      returnCode = [self bindBlob:[fldObj bytes]
                 withSqlStatement:sqlStatement
                   forParamNumber:bindIndex
                         ofLength:[fldObj length]
                      asTransient:YES];
   }
   else  if ([fldObj isKindOfClass:[UIImage class]])  {
      NSData  *imgData = UIImageJPEGRepresentation (fldObj, 1.);
      returnCode = [self bindBlob:[imgData bytes]
                 withSqlStatement:sqlStatement
                   forParamNumber:bindIndex
                         ofLength:[imgData length]
                      asTransient:YES];
   }
   else  if ([fldObj isKindOfClass:[NSDate class]])  {
      if (fldDesc->sqlType == SQLITE_TEXT)  {
         NSString  *dateStr = [DataUtils iso8601StringFromDate:fldObj];
         returnCode = [self bindText:[[self sqlStringWithString:dateStr] UTF8String]
                    withSqlStatement:sqlStatement
                      forParamNumber:bindIndex
                         asTransient:YES];
      }
      else  if (fldDesc->sqlType == SQLITE_FLOAT)
         returnCode = sqlite3_bind_double (sqlStatement, bindIndex, [fldObj timeIntervalSince1970]);
   }
   else  if ([fldObj isKindOfClass:[NSNumber class]])  {
      if (!strcmp([fldObj objCType], @encode(BOOL)) && fldDesc->sqlType == SQLITE_INTEGER)  {
         sqlite3_bind_int (sqlStatement, bindIndex, ([fldObj boolValue] ? 1 : 0));
      }
      else  if (!strcmp([fldObj objCType], @encode(int)) && fldDesc->sqlType == SQLITE_INTEGER) {
         returnCode = sqlite3_bind_int64 (sqlStatement, bindIndex, [fldObj longValue]);
      }
      else  if (!strcmp([fldObj objCType], @encode(long)) && fldDesc->sqlType == SQLITE_INTEGER)  {
         if (sizeof(long)==sizeof(int))
            returnCode = sqlite3_bind_int (sqlStatement, bindIndex, (int)[fldObj longValue]);
         else
            returnCode = sqlite3_bind_int64 (sqlStatement, bindIndex, [fldObj longValue]);
      }
      else  if (!strcmp([fldObj objCType], @encode(long long)) && fldDesc->sqlType == SQLITE_INTEGER) {
         returnCode = sqlite3_bind_int64 (sqlStatement, bindIndex, [fldObj longLongValue]);
      }
      else  if (!strcmp([fldObj objCType], @encode(float)) && fldDesc->sqlType == SQLITE_FLOAT) {
         returnCode = sqlite3_bind_double (sqlStatement, bindIndex, [fldObj floatValue]);
      }
      else  if (!strcmp([fldObj objCType], @encode(double)) && fldDesc->sqlType == SQLITE_FLOAT) {
         returnCode = sqlite3_bind_double (sqlStatement, bindIndex, [fldObj doubleValue]);
      }
      else {
         returnCode = sqlite3_bind_text (sqlStatement, bindIndex, [[fldObj description] UTF8String], -1, SQLITE_STATIC);
      }
   }
   else  if ([fldObj isKindOfClass:[NSNull class]])
      returnCode = sqlite3_bind_null(sqlStatement, bindIndex);
   else  if ([fldObj isKindOfClass:[NSArray class]])  {
      NSString  *tmpStr = [fldObj componentsJoinedByString:kArrayFldSeparator];
      returnCode = sqlite3_bind_text (sqlStatement, bindIndex, [tmpStr UTF8String], -1, SQLITE_STATIC);
   }
   
#ifdef kSQLITE_LOG_
   if (returnCode != SQLITE_OK)
      NSLog (@"bindSingleObject:...:usingFldDescription: - code: %d, bidx: %d,  %@: %@", returnCode, bindIndex, fldDesc->fldName, [fldObj description]);
#endif

   return (returnCode);
}

- (int)bindStorageObject:(id)storageObj
          toSqlStatement:(sqlite3_stmt *)sqlStatement
          fromBindOffset:(int)bOffset
    usingFldDescriptions:(SQLFldDescription *)fldDescCArray
 includingOnlySearchable:(BOOL)searchableOnly
     includingOnlyFields:(NSArray *)fldNamesToMatchOrNil
      returningUsedCount:(int *)boundCnt
{
   id   fldObj;
   int  pix, i;
   int  returnCode;
   
   bOffset += 1;   // bind params start from 1, so add it to whatever we got
   
   for (pix=i=0; fldDescCArray[i].fldName; i++)  {
      
      // if (onlyFieldOrNil && (![fldDescCArray[i].fldName isEqualToString:onlyFieldOrNil]))
      //    continue;
      
      if (searchableOnly && !(fldDescCArray[i].fldFlags & kFldDescFlagSearchableKey))
         continue;
      
      if (fldNamesToMatchOrNil && ![fldNamesToMatchOrNil containsObject:fldDescCArray[i].fldName])
         continue;
      
      // pix is incremented at the end of the loop
      
      if ([storageObj isKindOfClass:[NSDictionary class]])
         fldObj = [storageObj objectForKey:fldDescCArray[i].fldName]; // Dict
      else
         fldObj = [storageObj valueForKey:fldDescCArray[i].fldName];  // KVC
      
      
      returnCode = [self bindSingleObject:fldObj
                           toSqlStatement:sqlStatement
                              atBindIndex:bOffset + pix
                      usingFldDescription:&fldDescCArray[i]];
      
      pix++;
   }
   
   if (boundCnt)
      *boundCnt = pix;  // pix starts at zero, so 3 means 3 items bound
   
   return (returnCode);
}

- (int)bindToSqlStatement:(sqlite3_stmt *)sqlStatement
           fromBindOffset:(int)bOffset
     usingFldDescriptions:(SQLFldDescription *)fldDescCArray
  includingOnlySearchable:(BOOL)searchableOnly
         searchFieldNames:(NSArray *)fldNamesToMatch
     andSearchFieldValues:(NSArray *)fldValuesToMatch
       returningUsedCount:(int *)boundCnt
{
    int  i, pix, aix;
    int  returnCode = SQLITE_ERROR;
   
    id   fldObj;
   
    bOffset += 1;   // bind params start from 1, so add it to whatever we got
    
    if (!fldNamesToMatch && fldValuesToMatch)  {
        pix = 0;
        
        fldObj = [fldValuesToMatch objectAtIndex:0];
        
        returnCode = [self bindSingleObject:fldObj
                             toSqlStatement:sqlStatement
                                atBindIndex:bOffset + pix
                        usingFldDescription:&internalSQLFields[0]];
        pix++;
    }
    else  {
        for (pix=i=0; fldDescCArray[i].fldName; i++)  {
            if ((!searchableOnly || (fldDescCArray[i].fldFlags & kFldDescFlagSearchableKey)) &&
                ([fldNamesToMatch containsObject:fldDescCArray[i].fldName]))  {
                
                aix    = [fldNamesToMatch indexOfObject:fldDescCArray[i].fldName];
                fldObj = [fldValuesToMatch objectAtIndex:aix];
                
                if (searchableOnly && (fldObj == [NSNull null]))  // where clause
                    continue;
                
                returnCode = [self bindSingleObject:fldObj
                                     toSqlStatement:sqlStatement
                                        atBindIndex:bOffset + pix
                                usingFldDescription:&fldDescCArray[i]];
                pix++;
                
                if (returnCode != SQLITE_OK)
                    break;
            }
        }
    }   
    if (boundCnt)
        *boundCnt = pix;  // pix starts at zero, so 3 means 3 items bound
   
    return (returnCode);
}

#pragma mark -

- (id)extractSingleObjectFromSqlStatement:(sqlite3_stmt *)sqlStatement
                           forColumnIndex:(int)columnIndex  // zero based
                      usingFldDescription:(SQLFldDescription *)fldDesc
{
   id                    retObject = nil, tmpObject;
   const unsigned char  *rowText;
   const char           *rowData;
   NSInteger             dataLength;
      
   if (fldDesc->sqlType == SQLITE_TEXT)  {
      
      if ((rowText = [self textWithSqlStatement:sqlStatement forColumnIndex:columnIndex]))  {
         tmpObject = [self stringWithSqlString:[NSString stringWithCString:(const char *)rowText encoding:NSUTF8StringEncoding]];
         if (!tmpObject)
            retObject = nil;
         else  if ([fldDesc->className isEqual:kSTRING_Class])
            retObject = tmpObject;
         else  if ([fldDesc->className isEqual:kDATE_Class])
            retObject = [DataUtils dateFromIso8601String:tmpObject];
         else  if ([fldDesc->className isEqual:kARRAY_Class])
            retObject = [(NSString *)tmpObject componentsSeparatedByString:kArrayFldSeparator];
#ifdef kSQLITE_LOG_
         else
            NSLog (@"Unknown SQLITE_TEXT field class for: %@", fldDesc->fldName);
#endif
      }
   }
   else  if (fldDesc->sqlType == SQLITE_BLOB)  {
      if ((rowData = [self blobWithSqlStatement:sqlStatement forColumnIndex:columnIndex andLength:&dataLength]))  {
         tmpObject = [NSData dataWithBytes:rowData length:dataLength];
         if (!tmpObject)
            retObject = nil;
         else  if ([fldDesc->className isEqual:kDATA_Class])
            retObject = tmpObject;
         else  if ([fldDesc->className isEqual:kIMAGE_Class])
            retObject = [UIImage imageWithData:tmpObject];
#ifdef kSQLITE_LOG_
         else
            NSLog (@"Unknown SQLITE_BLOB field class for: %@", fldDesc->fldName);
#endif
      }
   }
   else  if (fldDesc->sqlType == SQLITE_INTEGER)  {
      retObject = [NSNumber numberWithInt:[self intWithSqlStatement:sqlStatement forColumnIndex:columnIndex]];
   }
   else  if (fldDesc->sqlType == SQLITE_FLOAT)  {
      double  tmpDouble = sqlite3_column_double (sqlStatement, columnIndex);
      if ([fldDesc->className isEqual:kDATE_Class])
         retObject = [NSDate dateWithTimeIntervalSince1970:tmpDouble];
      else
         retObject = [NSNumber numberWithDouble:tmpDouble];
   }
#ifdef kSQLITE_LOG_
   else
      NSLog (@"Unknown type field: %@", fldDesc->fldName);
#endif
   
   return (retObject);
}

#ifdef _NIJE_
- (id)extractSingleObjectFromSqlStatement:(sqlite3_stmt *)sqlStatement
                           forColumnIndex:(int)columnIndex  // zero based
                      usingFldDescription:(SQLFldDescription *)fldDesc
{
   id                    retObject = nil;
   const unsigned char  *rowText;
   const char           *rowData;
   NSInteger             dataLength;
   
   if (fldDesc->sqlType == SQLITE_TEXT)  {
      
      if (rowText = [self textWithSqlStatement:sqlStatement forColumnIndex:columnIndex])
         retObject = [self stringWithSqlString:[NSString stringWithCString:(const char *)rowText encoding:NSUTF8StringEncoding]];
   }
   else  if (fldDesc->sqlType == SQLITE_BLOB)  {
      if (rowData = [self blobWithSqlStatement:sqlStatement forColumnIndex:columnIndex andLength:&dataLength])
         retObject = [NSData dataWithBytes:rowData length:dataLength];
   }
   else  if (fldDesc->sqlType == SQLITE_INTEGER)
      retObject = [NSNumber numberWithInt:[self intWithSqlStatement:sqlStatement forColumnIndex:columnIndex]];
	else  if (fldDesc->sqlType == SQLITE_FLOAT)
      retObject = [NSNumber numberWithDouble:sqlite3_column_double(sqlStatement, columnIndex)];
   
   return (retObject);
}
#endif

#pragma mark -

- (int)addStorageObject:(id)storageObj
              intoTable:(NSString *)tableName
   usingFldDescriptions:(SQLFldDescription *)fldDescCArray
         returningRowId:(NSInteger *)retRowId
{
   // NSString      *sqlString = [NSString stringWithFormat:@"INSERT INTO %s (name, title, image, thumb, comment) values (?,?,?,?,?)", kTableName];
   if (retRowId)
      *retRowId = 0;
   
   NSString      *sqlString = [SQLHelper sqlInsertionStringForTable:tableName withFldDescriptions:fldDescCArray];
   
   sqlite3_stmt  *sqlStatement;
   
   int            rowId, returnCode = [self prepare:sqlString sqlStatement:&sqlStatement];
   
#ifdef kSQLITE_LOG_
   NSLog (@"addStorageObject:intoTable:usingFldDescriptions: - code: %d str: %@", returnCode, sqlString);
#endif
   
   if (returnCode == SQLITE_OK)  {
      returnCode = [self bindStorageObject:storageObj
                            toSqlStatement:sqlStatement
                            fromBindOffset:0
                      usingFldDescriptions:fldDescCArray
                   includingOnlySearchable:NO
                       includingOnlyFields:nil
                        returningUsedCount:nil];
   }
   
   returnCode = [self stepWithSqlStatement:sqlStatement];
   if (returnCode == SQLITE_DONE)
      returnCode = SQLITE_OK;
   
   if (returnCode == SQLITE_OK)  {
      rowId = sqlite3_last_insert_rowid (self.dbHandle);
      if (retRowId)
         *retRowId = rowId;
   }
   
   // if (returnCode == SQLITE_OK)
   returnCode = [self finalizeSqlStatement:sqlStatement];
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Added one record to SQLite db - ROWID = %d!", rowId);
#endif
   
   return (returnCode);
}

- (int)updateStorageObject:(id)newStorageObjOrNil
      byReplacingOldObject:(id)oldStorageObjOrNil
    orJustSearchFieldNames:(NSArray *)fldNamesToMatch
      andSearchFieldValues:(NSArray *)fldValuesToMatch
                   inTable:(NSString *)tableName
      usingFldDescriptions:(SQLFldDescription *)fldDescCArray
     withOnlyFieldToUpdate:(NSString *)onlyFieldOrNil
               andItsValue:(id)onlyFieldValueOrNil
{
   // NSString      *sqlString = [NSString stringWithFormat:@"UPDATE %s SET %s = ? WHERE name = ?", kTableName, thumbFlag ? "thumb" : "image"];
   
   int            qMarksCnt = 0;
   NSString      *sqlString = [SQLHelper sqlUpdatingStringForTable:tableName
                                                  withSearchFields:fldNamesToMatch
                                              andSearchFieldValues:fldValuesToMatch
                                              usingFldDescriptions:fldDescCArray
                                              andOnlyFieldToUpdate:onlyFieldOrNil   // @"thumb" or @"image" for example
                                                returningUsedCount:&qMarksCnt];
   
   sqlite3_stmt  *sqlStatement;
   
   int            bindingCnt1, bindingCnt2;
   int            returnCode = [self prepare:sqlString sqlStatement:&sqlStatement];
   
#ifdef kSQLITE_LOG_
   NSLog (@"updateStorageObject:...:andItsValue: - code: %d str: %@", returnCode, sqlString);
#endif
   // First part, bind items to be updated
   
   if (returnCode == SQLITE_OK)  {
      if (!newStorageObjOrNil)  {
         NSArray  *fldValues = [NSArray arrayWithObject:onlyFieldValueOrNil ? onlyFieldValueOrNil : [NSNull null]];
         returnCode = [self bindToSqlStatement:sqlStatement
                                fromBindOffset:0
                          usingFldDescriptions:fldDescCArray
                       includingOnlySearchable:NO                                              // POOR CHOICE OF NAMING!
                              searchFieldNames:[NSArray arrayWithObject:onlyFieldOrNil]        // Not search fields...
                          andSearchFieldValues:fldValues                                       // The fields we are updateing
                            returningUsedCount:&bindingCnt1];
      }
      else
         returnCode = [self bindStorageObject:newStorageObjOrNil
                               toSqlStatement:sqlStatement
                               fromBindOffset:0  // start at zero
                         usingFldDescriptions:fldDescCArray
                      includingOnlySearchable:NO
                          includingOnlyFields:(onlyFieldOrNil ? [NSArray arrayWithObject:onlyFieldOrNil] : nil)
                           returningUsedCount:&bindingCnt1];
   }
   
   // Second part, bind items in WHERE clause -> includingOnlySearchable:YES
   
   if (returnCode == SQLITE_OK)  {
      if (fldNamesToMatch || (!fldNamesToMatch && fldValuesToMatch))  // 2nd case with id
         returnCode = [self bindToSqlStatement:sqlStatement
                                fromBindOffset:bindingCnt1  // one field bound above
                          usingFldDescriptions:fldDescCArray
                       includingOnlySearchable:YES
                              searchFieldNames:fldNamesToMatch
                          andSearchFieldValues:fldValuesToMatch
                            returningUsedCount:&bindingCnt2];
      else
         returnCode = [self bindStorageObject:oldStorageObjOrNil
                               toSqlStatement:sqlStatement
                               fromBindOffset:bindingCnt1  // one field bound above
                         usingFldDescriptions:fldDescCArray
                      includingOnlySearchable:YES
                          includingOnlyFields:nil
                           returningUsedCount:&bindingCnt2];
   }
   
   if (qMarksCnt != bindingCnt1 + bindingCnt2)
      NSLog (@"Wrong binding count in updateStorageObject:...:andItsValue: - %d vs %d", qMarksCnt, bindingCnt1 + bindingCnt2);
   
   returnCode = [self stepWithSqlStatement:sqlStatement];
   if (returnCode == SQLITE_DONE)
      returnCode = SQLITE_OK;
   
   // if (returnCode == SQLITE_OK)
   returnCode = [self finalizeSqlStatement:sqlStatement];
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Updated one record in SQLite db!");
#endif
   
   return (returnCode);
}

// Pass the object itself or just field names and values to be used in where clause

- (int)deleteRecordInTable:(NSString *)tableName
      usingFldDescriptions:(SQLFldDescription *)fldDescCArray
         withStorageObject:(id)storageObjOrNil
    orJustSearchFieldNames:(NSArray *)fldNamesToMatch
      andSearchFieldValues:(NSArray *)fldValuesToMatch
{
   // NSString  *sqlString = [NSString stringWithFormat:@"DELETE FROM %s WHERE name = ?", kTableName];
   
   int            qMarksCnt, bindingCnt;
   NSString      *sqlString = [SQLHelper sqlDeletionStringForTable:tableName
                                                  withSearchFields:fldNamesToMatch
                                              andSearchFieldValues:fldValuesToMatch        // just to check for nulls
                                              usingFldDescriptions:fldDescCArray
                                                returningUsedCount:&qMarksCnt];
   
   sqlite3_stmt  *sqlStatement;
   
   int            returnCode = [self prepare:sqlString sqlStatement:&sqlStatement];
   
#ifdef kSQLITE_LOG_
   NSLog (@"deleteRecordInTable:...:andSearchFieldValues: - code: %d str: %@", returnCode, sqlString);
#endif

   // Bind items in WHERE clause -> includingOnlySearchable:YES
   
   if (returnCode == SQLITE_OK)  {
      if (fldNamesToMatch || (!fldNamesToMatch && fldValuesToMatch))
         returnCode = [self bindToSqlStatement:sqlStatement
                                fromBindOffset:0
                          usingFldDescriptions:fldDescCArray
                       includingOnlySearchable:YES
                              searchFieldNames:fldNamesToMatch
                          andSearchFieldValues:fldValuesToMatch
                            returningUsedCount:&bindingCnt];
      else
         returnCode = [self bindStorageObject:storageObjOrNil
                               toSqlStatement:sqlStatement
                               fromBindOffset:0
                         usingFldDescriptions:fldDescCArray
                      includingOnlySearchable:YES
                          includingOnlyFields:nil
                           returningUsedCount:&bindingCnt];
   }
   
   if (qMarksCnt != bindingCnt)
      NSLog (@"Wrong binding count in deleteRecordInTable:...:andSearchFieldValues: - %d vs %d", qMarksCnt, bindingCnt);

   returnCode = [self stepWithSqlStatement:sqlStatement];
   if (returnCode == SQLITE_DONE)
      returnCode = SQLITE_OK;
   
   // if (returnCode == SQLITE_OK)
   returnCode = [self finalizeSqlStatement:sqlStatement];
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Deleted one record to SQLite db!");
#endif
   
   return (returnCode);
}

// CALLING IT:
// 1. pass an SQLSelectParams instance with WHERE, SORTED BY, LIMIT and OFFSET params
// 2. just pass one or more fields to match
// 3. if searching by id: fldNamesToMatch is nil and fldValuesToMatch has one item, desired id
// RESULTS:
// 1. recieve one field - onlyFieldOrNil
// 2. recieve array of objects

- (id)selectObjectFromTable:(NSString *)tableName
       usingFldDescriptions:(SQLFldDescription *)fldDescCArray
           withSelectParams:(SQLSelectParams *)selParams
     orJustSearchFieldNames:(NSArray *)fldNamesToMatch
       andSearchFieldValues:(NSArray *)fldValuesToMatch
       andOnlyFieldToSelect:(NSString *)onlyFieldOrNil
            givinReturnCode:(int *)returnCodeOrNil;
{
   id  retObject = [self selectObjectsArrayFromTable:tableName
                                usingFldDescriptions:fldDescCArray
                                    withSelectParams:selParams
                              orJustSearchFieldNames:fldNamesToMatch
                                andSearchFieldValues:fldValuesToMatch
                                andOnlyFieldToSelect:onlyFieldOrNil
                                  singleResultNeeded:YES
                                     givinReturnCode:returnCodeOrNil];
   
   if ([retObject isKindOfClass:[NSArray class]])  {
      NSArray  *retArray = (NSArray *)retObject;
      
      return ([retArray count] ? [retArray objectOrNilAtIndex:0] : nil);
   }

   if (retObject)
      NSLog (@"selectObjectFromTable: Class: %@!", [retObject class]);
   
   return (retObject);
}

- (id)selectObjectsArrayFromTable:(NSString *)tableName
             usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                 withSelectParams:(SQLSelectParams *)selParams
           orJustSearchFieldNames:(NSArray *)fldNamesToMatch
             andSearchFieldValues:(NSArray *)fldValuesToMatch
             andOnlyFieldToSelect:(NSString *)onlyFieldOrNil
               singleResultNeeded:(BOOL)singleResultFlag
                  givinReturnCode:(int *)returnCodeOrNil;
{
   // NSString      *preparedName = [dbFileHelper sqlStringWithString:iName];
   // NSString      *sqlString = [NSString stringWithFormat:@"SELECT %s FROM %s WHERE name = ?", "image", kTableName];
   
   int            qMarksCnt, bindingCnt;
   id             fldObj, retObject = nil;
   
   NSString      *sqlString = [SQLHelper sqlSelectionStringForTable:tableName
                                                   withSelectParams:selParams
                                                 orJustSearchFields:fldNamesToMatch
                                              withSearchFieldValues:fldValuesToMatch        // just to check for nulls
                                               usingFldDescriptions:fldDescCArray
                                               andOnlyFieldToSelect:onlyFieldOrNil
                                                 returningUsedCount:&qMarksCnt];
   
   sqlite3_stmt  *sqlStatement;
   
   // When I select one field only, it is at the zero position
   // When I select all of the fields, it is all except the id field at zero position
   
   int            rcvParamIndexOffset = onlyFieldOrNil ? 0 : 1;  // skip id
   int            i, pix;
   int            returnCode = [self prepare:sqlString sqlStatement:&sqlStatement];
   
#ifdef kSQLITE_LOG_
   NSLog (@"selectObjectFromTable:...:andOnlyFieldToSelect: - code: %d str: %@", returnCode, sqlString);
#endif
   
   if (fldNamesToMatch)  {
      returnCode = [self bindToSqlStatement:sqlStatement
                             fromBindOffset:0
                       usingFldDescriptions:fldDescCArray
                    includingOnlySearchable:YES
                           searchFieldNames:fldNamesToMatch
                       andSearchFieldValues:fldValuesToMatch
                         returningUsedCount:&bindingCnt];
      
      if (qMarksCnt != bindingCnt)
         NSLog (@"Wrong binding count in selectObjectFromTable:...:andItsValue: - %d vs %d", qMarksCnt, bindingCnt);
   }
   
   NSMutableArray  *retArray = [NSMutableArray array];
   
   returnCode = [self stepWithSqlStatement:sqlStatement];
   
   // if onlyFieldOrNil case, just grab the object and bail
   // otherwise, form an array of objects, leave the caller to make sense of it
   
   if (returnCode == SQLITE_ROW)  do  {  // was OK, because before we had ROW->OK
      
      // To fetch id, we do it here, before other fields and we put it into array as Number at index 0
      // and yes, only when no onlyFieldOrNil, when we had "SELECT *" above
      
      if (!onlyFieldOrNil)  {
         retObject = [NSMutableArray array];
         // First, grab ID
         fldObj = [self extractSingleObjectFromSqlStatement:sqlStatement
                                             forColumnIndex:0
                                        usingFldDescription:&internalSQLFields[0]];
         [(NSMutableArray *)retObject addObjectOrNull:fldObj];
      }
      
      for (pix=i=0; fldDescCArray[i].fldName; i++)  {
         if (onlyFieldOrNil && ![onlyFieldOrNil isEqualToString:fldDescCArray[i].fldName])
            continue;
         
         fldObj = [self extractSingleObjectFromSqlStatement:sqlStatement
                                             forColumnIndex:pix+rcvParamIndexOffset
                                        usingFldDescription:&fldDescCArray[i]];
         if (onlyFieldOrNil)  {
            retObject = fldObj;
            break;
         }
         else
            [(NSMutableArray *)retObject addObjectOrNull:fldObj];
         pix++;
      }
      
      [retArray addObjectOrNull:retObject];
      
      if (singleResultFlag)
         returnCode = SQLITE_DONE;
      else
         returnCode = [self stepWithSqlStatement:sqlStatement];
      
   } while (returnCode == SQLITE_ROW);
   
   if (returnCode == SQLITE_DONE)
      returnCode = SQLITE_OK;
#ifdef kSQLITE_LOG_
   else
      NSLog (@"Failed select: %d", returnCode);
#endif
   
   // if (returnCode == SQLITE_OK)
   returnCode = [self finalizeSqlStatement:sqlStatement];
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Found one image in SQLite db!");
#endif
   
   if (returnCodeOrNil)
      *returnCodeOrNil = returnCode;
   
   return ([retArray count] ? retArray : nil);
}

#pragma mark -

// CALLING IT:
// 1. pass an SQLSelectParams instance with WHERE, SORTED BY, LIMIT and OFFSET params
// 2. just pass one or more fields to match
// RESULTS:
// 1. recieve count of selected items

// NOT USED AT THE MOMENT!

- (int)countOfItemsFromTable:(NSString *)tableName
        usingFldDescriptions:(SQLFldDescription *)fldDescCArray
            withSelectParams:(SQLSelectParams *)selParams
      orJustSearchFieldNames:(NSArray *)fldNamesToMatch
        andSearchFieldValues:(NSArray *)fldValuesToMatch
{
   // NSString      *preparedName = [dbFileHelper sqlStringWithString:iName];
   // NSString      *sqlString = [NSString stringWithFormat:@"SELECT %s FROM %s WHERE name = ?", "image", kTableName];
   
   int            qMarksCnt, bindingCnt, retValue = 0;
   // id             fldObj, retObject = nil;
   
   NSString      *onlyFieldOrNil = @"id";  // because we only need count of them
   
   NSString      *sqlInnerString = [SQLHelper sqlSelectionStringForTable:tableName
                                                        withSelectParams:selParams
                                                      orJustSearchFields:fldNamesToMatch
                                                   withSearchFieldValues:fldValuesToMatch        // just to check for nulls
                                                    usingFldDescriptions:fldDescCArray
                                                    andOnlyFieldToSelect:onlyFieldOrNil
                                                      returningUsedCount:&qMarksCnt];
   
   NSString      *sqlString = [NSString stringWithFormat:@"select count(*) FROM (%@)", sqlInnerString];
   
   sqlite3_stmt  *sqlStatement;
   
   // int            rcvParamIndexOffset = onlyFieldOrNil ? 0 : 1;  // skip id if fetching all, but here we do need first
   // int            i, pix;
   int            returnCode = [self prepare:sqlString sqlStatement:&sqlStatement];
   
#ifdef kSQLITE_LOG_
   NSLog (@"countOfItemsFromTable:...:andSearchFieldValues: - code: %d str: %@", returnCode, sqlString);
#endif
   
   if (fldNamesToMatch)  {
      returnCode = [self bindToSqlStatement:sqlStatement
                             fromBindOffset:0
                       usingFldDescriptions:fldDescCArray
                    includingOnlySearchable:YES
                           searchFieldNames:fldNamesToMatch
                       andSearchFieldValues:fldValuesToMatch
                         returningUsedCount:&bindingCnt];
      
      if (qMarksCnt != bindingCnt)
         NSLog (@"Wrong binding count in selectObjectFromTable:...:andItsValue: - %d vs %d", qMarksCnt, bindingCnt);
   }
   
   returnCode = [self stepWithSqlStatement:sqlStatement];
   
   if (returnCode == SQLITE_ROW)
      returnCode = SQLITE_OK;
   
   // if onlyFieldOrNil case, just grab the object and bail
   // otherwise, form an array of objects, leave the caller to make sense of it
   
   if (returnCode == SQLITE_OK)
      retValue = [self intWithSqlStatement:sqlStatement forColumnIndex:0];  // The whole point!

#ifdef kSQLITE_LOG_
   else
      NSLog (@"Failed select count(): %d", returnCode);
#endif
   
   // if (returnCode == SQLITE_OK)
   returnCode = [self finalizeSqlStatement:sqlStatement];
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Have count in SQLite db!");
#endif
   
   return (retValue);
}

- (int)countOfRecordsInTable:(NSString *)tableName givingReturnCode:(int *)returnCodeOrNil
{
   // NSString      *preparedName = [dbFileHelper sqlStringWithString:iName];
   NSString      *sqlString = [NSString stringWithFormat:@"SELECT count(*) FROM (SELECT id FROM %@)", tableName];
   
   int            retValue = 0;
      
   sqlite3_stmt  *sqlStatement;
   
   int            returnCode = [self prepare:sqlString sqlStatement:&sqlStatement];
   
#ifdef kSQLITE_LOG_
   NSLog (@"countOfRecordsInTable: - code: %d str: %@", returnCode, sqlString);
#endif
      
   returnCode = [self stepWithSqlStatement:sqlStatement];
   
   if (returnCode == SQLITE_ROW)
      returnCode = SQLITE_OK;
      
   if (returnCode == SQLITE_OK)
      retValue = [self intWithSqlStatement:sqlStatement forColumnIndex:0];  // The whole point!
   
#ifdef kSQLITE_LOG_
   else
      NSLog (@"Failed select count(): %d", returnCode);
#endif
   
   // if (returnCode == SQLITE_OK)
   returnCode = [self finalizeSqlStatement:sqlStatement];
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Have count in SQLite db!");
#endif
   
   if (returnCodeOrNil)
      *returnCodeOrNil = returnCode;
   
   return (retValue);
}

- (int)countOfColumnsInTable:(NSString *)tableName
         includingPrimaryKey:(BOOL)primKeyFlag
            givingReturnCode:(int *)returnCodeOrNil;
{
   // NSString      *preparedName = [dbFileHelper sqlStringWithString:iName];
   NSString      *sqlString = [NSString stringWithFormat:@"PRAGMA table_info('%@')", tableName];
   
   int            retValue = 0;
   
   char         **rowp;
   int            nrows, ncols;
   
   // The result will have one row for each column in the table!
   
   int            returnCode = sqlite3_get_table (self.dbHandle, [sqlString UTF8String], &rowp, &nrows, &ncols, NULL);
      
#ifdef kSQLITE_LOG_
   NSLog (@"countOfColumnsInTable: - code: %d str: %@", returnCode, sqlString);
#endif
      
   if (returnCode == SQLITE_ROW)
      returnCode = SQLITE_OK;
   
   if (returnCode == SQLITE_OK)  {
      retValue = nrows;  // The whole point! (and not ncols)
      if (!primKeyFlag)
         retValue--;
   }
   
#ifdef kSQLITE_LOG_
   else
      NSLog (@"Failed select count(): %d", returnCode);
#endif
   
#ifdef kLOG_SQLITE_
   if (returnCode == SQLITE_OK)
      NSLog (@"Have count in SQLite db!");
#endif
   
   sqlite3_free_table (rowp);

   if (returnCodeOrNil)
      *returnCodeOrNil = returnCode;
   
   return (retValue);
}

@end

#pragma mark -

// .........................................................................................................
// ......................................................................................... SQLSelectParams
// .........................................................................................................

@implementation SQLSelectParams

#pragma mark -

@synthesize  whereClause, orderByClause, limitCnt, offsetCnt, distinctFlag;

#pragma mark -

- (id)init
{
   // Dont call us (We'll call you)
   
   return (nil);
}

- (id)initWithWhereClause:(NSString *)where
            orderByClause:(NSString *)orderBy
               limitCount:(int)limit
              offsetCount:(int)offset
                 distinct:(BOOL)distFlag;
{
   if (self = [super init])  {
      self.whereClause = where;
      self.orderByClause = orderBy;
      
      self.limitCnt = limit;
      self.offsetCnt = offset;
      self.distinctFlag = distFlag;
   }
   
   return (self);
}

- (void)dealloc
{
   [whereClause release];
   [orderByClause release];
   
   [super dealloc];
}

#pragma mark -

+ (SQLSelectParams *)selectParamsForWhereClause:(NSString *)where
{
   SQLSelectParams  *tmpParams = [[SQLSelectParams alloc] initWithWhereClause:where
                                                                orderByClause:nil
                                                                   limitCount:0
                                                                  offsetCount:0
                                                                     distinct:NO];
   
   return ([tmpParams autorelease]);
}

+ (SQLSelectParams *)selectParamsForDistinctSelection
{
   SQLSelectParams  *tmpParams = [[SQLSelectParams alloc] initWithWhereClause:nil
                                                                orderByClause:nil
                                                                   limitCount:0
                                                                  offsetCount:0
                                                                     distinct:YES];
   
   return ([tmpParams autorelease]);
}

#pragma mark -
#pragma mark DISTINCT USAGE EXAMPLE

/* USAGE:

- (NSArray *)distinctAuthorNamesGivinReturnCode:(int *)retCodeOrNil
{
   SQLSelectParams  *selParams = [SQLSelectParams selectParamsForDistinctSelection];
   
   NSArray  *authorNames = [dbSQLHelper selectObjectsArrayFromTable:[NSString stringWithFormat:@"%s", kTableName]
                                               usingFldDescriptions:&imgCacheSQLFields[0]
                                                   withSelectParams:selParams
                                             orJustSearchFieldNames:nil
                                               andSearchFieldValues:nil
                                               andOnlyFieldToSelect:kAuthornameColumnKey
                                                 singleResultNeeded:NO
                                                    givinReturnCode:retCodeOrNil];
   if (authorNames)
      NSLog (@"distinctAuthorNames: %d", [authorNames count]);
   
   return (authorNames);
}
 */

@end

// ---------------------------------------------------------------------
#pragma mark -
#pragma mark Categories
#pragma mark -
// ---------------------------------------------------------------------

#pragma mark -
#pragma mark NSDictionary (nullObjects)
#pragma mark -

@implementation NSMutableDictionary (nullObjects)

- (void)setObjectOrNull:(id)anObject forKey:(id)aKey;
{
    id  goodObject = anObject ? anObject : [NSNull null];
    
    [self setObject:goodObject forKey:aKey];
}

@end

@implementation NSDictionary (nullObjects)

- (id)objectOrNilForKey:(id)aKey;
{
    id  anObject = [self objectForKey:aKey];
    
    if (anObject == [NSNull null])
        return (nil);
    
    return (anObject);
}

@end

#pragma mark -
#pragma mark NSArray (nullObjects)
#pragma mark -

@implementation NSMutableArray (nullObjects)

- (void)addObjectOrNull:(id)anObject;
{
    id  goodObject = anObject ? anObject : [NSNull null];
    
    [self addObject:goodObject];
}

@end

@implementation NSArray (nullObjects)

- (id)objectOrNilAtIndex:(int)idx;
{
    id  anObject = [self objectAtIndex:idx];
    
    if (anObject == [NSNull null])
        return (nil);
    
    return (anObject);
}

@end
