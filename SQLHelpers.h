/*
 *  SQLHelpers.h
 *  iPPromet, taken from GlideShow
 *
 *  Created by Igor Delovski on 02.07.2011.
 *  Copyright 2011 Igor Delovski, Calyx d.o.o.. All rights reserved.
 *
 */

// TODO - Sep 2012
// Additional table in database
// selectObjectFromTable... -> selectObjectsFromTable... OBJECTS! array containing array

#import <UIKit/UIKit.h>

#import <sqlite3.h>


#ifndef  kStringYES
#define  kStringYES @"Yes"
#define  kStringNO  @"No"
#endif


#define  kArrayFldSeparator         @"--,--"
#define  kMaxBusyRetryCount         8

#define  kFldDescFlagUniqueKey      1
#define  kFldDescFlagSearchableKey  2
#define  kFldDescFlagNotNullKey     4


#define  kSTRING_Class    @"NSString"
#define  kDATE_Class      @"NSDate"
#define  kDATA_Class      @"NSData"
#define  kARRAY_Class     @"NSArray"
#define  kNUMBER_Class    @"NSNumber"
#define  kIMAGE_Class     @"UIImage"

@class  SQLSelectParams;

typedef struct  SQLFldDesc  {
   NSString  *fldName;    // use static strings so they don't need to be retained...
   NSString  *className;  // AH!
   int        sqlType;    // SQLITE_INTEGER, SQLITE_TEXT, etc.
   int        fldFlags;   // uniqueFlag, whereSearchFlag, etc.
   int        maxLen;     // optional, do what you want
} SQLFldDescription;


@interface SQLHelper : NSObject {
   sqlite3   *dbHandleFore;
   sqlite3   *dbHandleBack;
   NSString  *fileName;
}

@property (nonatomic, retain)  NSString  *fileName;

+ (int)createNewDatabase:(NSString *)filePath creatingTableWithSqlString:(NSString *)sqlStr errString:(NSString **)retErrStr;


- (id)initWithFilePath:(NSString *)filePath;
- (id)initWithFileInDocumentsDirectory:(NSString *)fName;
- (id)initWithFileInCacheDirectory:(NSString *)aFileName;

- (sqlite3 *)dbHandle;

- (NSString *)sqlStringWithString:(NSString *)origStr;
- (NSString *)stringWithSqlString:(NSString *)sqlStr;
- (BOOL)shouldContinueLoopingWithReturnCode:(int)retCode
                         andNumberOfRetries:(int)numberOfRetries;

- (int)execSqlString:(NSString *)sqlString errString:(NSString **)retErrStr;

- (int)prepare:(NSString *)sqlString sqlStatement:(sqlite3_stmt **)retStatement;
- (int)stepWithSqlStatement:(sqlite3_stmt *)statement;
- (int)finalizeSqlStatement:(sqlite3_stmt *)statement;

- (int)bindText:(const char *)txt withSqlStatement:(sqlite3_stmt *)statement forParamNumber:(NSInteger)paramNum asTransient:(BOOL)byRef;
- (int)bindInt:(int)intVal withSqlStatement:(sqlite3_stmt *)statement forParamNumber:(NSInteger)paramNum;
- (int)bindBlob:(const void *)ptr withSqlStatement:(sqlite3_stmt *)statement forParamNumber:(NSInteger)paramNum ofLength:(NSInteger)length asTransient:(BOOL)byRef;

- (const unsigned char *)textWithSqlStatement:(sqlite3_stmt *)statement forColumnIndex:(NSInteger)idx;
- (int)intWithSqlStatement:(sqlite3_stmt *)statement forColumnIndex:(NSInteger)idx;
- (const void *)blobWithSqlStatement:(sqlite3_stmt *)statement forColumnIndex:(NSInteger)idx andLength:(NSInteger *)retLength;

// ---- SQLFldDescription Handling ---

/*
+ (void)initializeFldDescription:(SQLFldDescription *)fldDescCArray
               withStorageObject:(id)storageObj;
*/

+ (NSString *)creationTypeStringForFldDescription:(SQLFldDescription *)fldDesc;
+ (NSString *)sqlWhereClauseStringWithSearchFields:(NSArray *)fldNamesToMatch
                             withSearchFieldValues:(NSArray *)fldValuesToMatch        // just to check for nulls
                              usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                                returningUsedCount:(int *)boundCnt;

+ (NSString *)sqlCreationStringForTable:(NSString *)tableName
                    withFldDescriptions:(SQLFldDescription *)fldDescCArray;
+ (NSString *)sqlColumnAppendingStringForTable:(NSString *)tableName
                            withFldDescription:(SQLFldDescription *)oneFldDesc;

+ (NSString *)sqlInsertionStringForTable:(NSString *)tableName
                     withFldDescriptions:(SQLFldDescription *)fldDescCArray;
+ (NSString *)sqlUpdatingStringForTable:(NSString *)tableName
                       withSearchFields:(NSArray *)fldNamesToMatchOrNil
                   andSearchFieldValues:(NSArray *)fldValuesToMatchOrNil  // just to check for nulls
                   usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                   andOnlyFieldToUpdate:(NSString *)onlyFieldOrNil
                     returningUsedCount:(int *)boundCnt;
+ (NSString *)sqlDeletionStringForTable:(NSString *)tableName
                       withSearchFields:(NSArray *)fldNamesToMatchOrNil
                   andSearchFieldValues:(NSArray *)fldValuesToMatchOrNil        // just to check for nulls
                   usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                     returningUsedCount:(int *)boundCnt;

- (int)bindSingleObject:(id)fldObj
         toSqlStatement:(sqlite3_stmt *)sqlStatement
            atBindIndex:(int)bindIndex  // one based
    usingFldDescription:(SQLFldDescription *)fldDesc;

- (int)bindStorageObject:(id)storageObj
          toSqlStatement:(sqlite3_stmt *)sqlStatement
          fromBindOffset:(int)bOffset
    usingFldDescriptions:(SQLFldDescription *)fldDescCArray
 includingOnlySearchable:(BOOL)searchableOnly
     includingOnlyFields:(NSArray *)fldNamesToMatchOrNil
      returningUsedCount:(int *)boundCnt;

- (int)bindToSqlStatement:(sqlite3_stmt *)sqlStatement
           fromBindOffset:(int)bOffset
     usingFldDescriptions:(SQLFldDescription *)fldDescCArray
  includingOnlySearchable:(BOOL)searchableOnly
         searchFieldNames:(NSArray *)fldNamesToMatch
     andSearchFieldValues:(NSArray *)fldValuesToMatch
       returningUsedCount:(int *)boundCnt;

- (id)extractSingleObjectFromSqlStatement:(sqlite3_stmt *)sqlStatement
                           forColumnIndex:(int)columnIndex  // zero based
                      usingFldDescription:(SQLFldDescription *)fldDesc;

#pragma mark -

- (int)addStorageObject:(id)storageObj
              intoTable:(NSString *)tableName
   usingFldDescriptions:(SQLFldDescription *)fldDescCArray
         returningRowId:(NSInteger *)retRowId;

- (int)updateStorageObject:(id)newStorageObjOrNil
      byReplacingOldObject:(id)oldStorageObjOrNil
    orJustSearchFieldNames:(NSArray *)fldNamesToMatch
      andSearchFieldValues:(NSArray *)fldValuesToMatch
                   inTable:(NSString *)tableName
      usingFldDescriptions:(SQLFldDescription *)fldDescCArray
     withOnlyFieldToUpdate:(NSString *)onlyFieldOrNil
               andItsValue:(id)onlyFieldValueOrNil;

- (int)deleteRecordInTable:(NSString *)tableName
      usingFldDescriptions:(SQLFldDescription *)fldDescCArray
         withStorageObject:(id)storageObjOrNil
    orJustSearchFieldNames:(NSArray *)fldNamesToMatch
      andSearchFieldValues:(NSArray *)fldValuesToMatch;

// Multiple results version, returns array of objects (could be array of arrays if not onlyFieldOrNil version)
- (id)selectObjectsArrayFromTable:(NSString *)tableName
             usingFldDescriptions:(SQLFldDescription *)fldDescCArray
                 withSelectParams:(SQLSelectParams *)selParams
           orJustSearchFieldNames:(NSArray *)fldNamesToMatch
             andSearchFieldValues:(NSArray *)fldValuesToMatch
             andOnlyFieldToSelect:(NSString *)onlyFieldOrNil
               singleResultNeeded:(BOOL)singleResultFlag
                  givinReturnCode:(int *)returnCodeOrNil;

// Single result version, calls above method internally
- (id)selectObjectFromTable:(NSString *)tableName
       usingFldDescriptions:(SQLFldDescription *)fldDescCArray
           withSelectParams:(SQLSelectParams *)selParams
     orJustSearchFieldNames:(NSArray *)fldNamesToMatch
       andSearchFieldValues:(NSArray *)fldValuesToMatch
       andOnlyFieldToSelect:(NSString *)onlyFieldOrNil
            givinReturnCode:(int *)returnCodeOrNil;


#pragma mark -

- (int)countOfItemsFromTable:(NSString *)tableName
        usingFldDescriptions:(SQLFldDescription *)fldDescCArray
            withSelectParams:(SQLSelectParams *)selParams
      orJustSearchFieldNames:(NSArray *)fldNamesToMatch
        andSearchFieldValues:(NSArray *)fldValuesToMatch;

- (int)countOfRecordsInTable:(NSString *)tableName givingReturnCode:(int *)returnCodeOrNil;
- (int)countOfColumnsInTable:(NSString *)tableName
         includingPrimaryKey:(BOOL)primKeyFlag
            givingReturnCode:(int *)returnCodeOrNil;

@end



@interface SQLSelectParams : NSObject {
   NSString  *whereClause;
   NSString  *orderByClause;
   int        limitCnt;
   int        offsetCnt;
   
   BOOL       distinctFlag;
}

@property (nonatomic, retain)  NSString  *whereClause;
@property (nonatomic, retain)  NSString  *orderByClause;

@property (nonatomic, assign)  int        limitCnt;
@property (nonatomic, assign)  int        offsetCnt;
@property (nonatomic, assign)  BOOL       distinctFlag;



- (id)initWithWhereClause:(NSString *)where
            orderByClause:(NSString *)orderBy
               limitCount:(int)limit
              offsetCount:(int)offset
                 distinct:(BOOL)distinctFlag;

+ (SQLSelectParams *)selectParamsForWhereClause:(NSString *)where;
+ (SQLSelectParams *)selectParamsForDistinctSelection;

@end

// ---------------------------------------------------------------------
#pragma mark -
#pragma mark Categories
#pragma mark -
// ---------------------------------------------------------------------

@interface NSMutableDictionary (nullObjects) 

- (void)setObjectOrNull:(id)anObject forKey:(id)aKey;

@end

@interface NSDictionary (nullObjects) 

- (id)objectOrNilForKey:(id)aKey;

@end

@interface NSMutableArray (nullObjects)

- (void)addObjectOrNull:(id)anObject;

@end

@interface NSArray (nullObjects)

- (id)objectOrNilAtIndex:(int)idx;

@end
