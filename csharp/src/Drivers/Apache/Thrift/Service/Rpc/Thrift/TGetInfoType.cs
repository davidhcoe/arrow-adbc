/**
 * <auto-generated>
 * Autogenerated by Thrift Compiler (0.21.0)
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 * </auto-generated>
 */
using System;

// targeting netstandard 2.x
#if(! NETSTANDARD2_0_OR_GREATER && ! NET6_0_OR_GREATER && ! NET472_OR_GREATER)
#error Unexpected target platform. See 'thrift --help' for details.
#endif

#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE0017  // object init can be simplified
#pragma warning disable IDE0028  // collection init can be simplified
#pragma warning disable IDE1006  // parts of the code use IDL spelling
#pragma warning disable CA1822   // empty DeepCopy() methods still non-static
#pragma warning disable CS0618   // silence our own deprecation warnings
#pragma warning disable IDE0083  // pattern matching "that is not SomeType" requires net5.0 but we still support earlier versions

namespace Apache.Hive.Service.Rpc.Thrift
{
  internal enum TGetInfoType
  {
    CLI_MAX_DRIVER_CONNECTIONS = 0,
    CLI_MAX_CONCURRENT_ACTIVITIES = 1,
    CLI_DATA_SOURCE_NAME = 2,
    CLI_FETCH_DIRECTION = 8,
    CLI_SERVER_NAME = 13,
    CLI_SEARCH_PATTERN_ESCAPE = 14,
    CLI_DBMS_NAME = 17,
    CLI_DBMS_VER = 18,
    CLI_ACCESSIBLE_TABLES = 19,
    CLI_ACCESSIBLE_PROCEDURES = 20,
    CLI_CURSOR_COMMIT_BEHAVIOR = 23,
    CLI_DATA_SOURCE_READ_ONLY = 25,
    CLI_DEFAULT_TXN_ISOLATION = 26,
    CLI_IDENTIFIER_CASE = 28,
    CLI_IDENTIFIER_QUOTE_CHAR = 29,
    CLI_MAX_COLUMN_NAME_LEN = 30,
    CLI_MAX_CURSOR_NAME_LEN = 31,
    CLI_MAX_SCHEMA_NAME_LEN = 32,
    CLI_MAX_CATALOG_NAME_LEN = 34,
    CLI_MAX_TABLE_NAME_LEN = 35,
    CLI_SCROLL_CONCURRENCY = 43,
    CLI_TXN_CAPABLE = 46,
    CLI_USER_NAME = 47,
    CLI_TXN_ISOLATION_OPTION = 72,
    CLI_INTEGRITY = 73,
    CLI_GETDATA_EXTENSIONS = 81,
    CLI_NULL_COLLATION = 85,
    CLI_ALTER_TABLE = 86,
    CLI_ORDER_BY_COLUMNS_IN_SELECT = 90,
    CLI_SPECIAL_CHARACTERS = 94,
    CLI_MAX_COLUMNS_IN_GROUP_BY = 97,
    CLI_MAX_COLUMNS_IN_INDEX = 98,
    CLI_MAX_COLUMNS_IN_ORDER_BY = 99,
    CLI_MAX_COLUMNS_IN_SELECT = 100,
    CLI_MAX_COLUMNS_IN_TABLE = 101,
    CLI_MAX_INDEX_SIZE = 102,
    CLI_MAX_ROW_SIZE = 104,
    CLI_MAX_STATEMENT_LEN = 105,
    CLI_MAX_TABLES_IN_SELECT = 106,
    CLI_MAX_USER_NAME_LEN = 107,
    CLI_OJ_CAPABILITIES = 115,
    CLI_XOPEN_CLI_YEAR = 10000,
    CLI_CURSOR_SENSITIVITY = 10001,
    CLI_DESCRIBE_PARAMETER = 10002,
    CLI_CATALOG_NAME = 10003,
    CLI_COLLATION_SEQ = 10004,
    CLI_MAX_IDENTIFIER_LEN = 10005,
    CLI_ODBC_KEYWORDS = 10006,
  }
}
