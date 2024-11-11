// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package snowflake

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const (
	defaultStatementQueueSize  = 200
	defaultPrefetchConcurrency = 10

	queryTemplateGetObjectsAll           = "get_objects_all.sql"
	queryTemplateGetObjectsDbSchemas     = "get_objects_dbschemas.sql"
	queryTemplateGetObjectsTables        = "get_objects_tables.sql"
	queryTemplateGetObjectsTerseCatalogs = "get_objects_terse_catalogs.sql"
)

//go:embed queries/*
var queryTemplates embed.FS

type snowflakeConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	QueryArrowStream(context.Context, string, ...driver.NamedValue) (gosnowflake.ArrowStreamLoader, error)
}

type connectionImpl struct {
	driverbase.ConnectionImplBase

	cn    snowflakeConn
	db    *databaseImpl
	ctor  gosnowflake.Connector
	sqldb *sql.DB

	activeTransaction bool
	useHighPrecision  bool
}

func escapeSingleQuoteForLike(arg string) string {
	if len(arg) == 0 {
		return arg
	}

	idx := strings.IndexByte(arg, '\'')
	if idx == -1 {
		return arg
	}

	var b strings.Builder
	b.Grow(len(arg))

	for {
		before, after, found := strings.Cut(arg, `'`)
		b.WriteString(before)
		if !found {
			return b.String()
		}

		if before[len(before)-1] != '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte('\'')
		arg = after
	}
}

func getQueryID(ctx context.Context, query string, driverConn any) (string, error) {
	rows, err := driverConn.(driver.QueryerContext).QueryContext(ctx, query, nil)
	if err != nil {
		return "", err
	}

	return rows.(gosnowflake.SnowflakeRows).GetQueryID(), rows.Close()
}

const (
	objSchemas   = "SCHEMAS"
	objDatabases = "DATABASES"
	objViews     = "VIEWS"
	objTables    = "TABLES"
	objObjects   = "OBJECTS"
)

func addLike(query string, pattern *string) string {
	if pattern != nil && len(*pattern) > 0 && *pattern != "%" && *pattern != ".*" {
		query += " LIKE '" + escapeSingleQuoteForLike(*pattern) + "'"
	}
	return query
}

func goGetQueryID(ctx context.Context, conn *sql.Conn, grp *errgroup.Group, objType string, catalog, dbSchema, tableName *string, outQueryID *string) {
	grp.Go(func() error {
		return conn.Raw(func(driverConn any) (err error) {
			query := "SHOW TERSE /* ADBC:getObjects */ " + objType
			switch objType {
			case objDatabases:
				query = addLike(query, catalog)
				query += " IN ACCOUNT"
			case objSchemas:
				query = addLike(query, dbSchema)

				if catalog == nil || isWildcardStr(*catalog) {
					query += " IN ACCOUNT"
				} else {
					query += " IN DATABASE " + quoteTblName(*catalog)
				}
			case objViews, objTables, objObjects:
				query = addLike(query, tableName)

				if catalog == nil || isWildcardStr(*catalog) {
					query += " IN ACCOUNT"
				} else {
					escapedCatalog := quoteTblName(*catalog)
					if dbSchema == nil || isWildcardStr(*dbSchema) {
						query += " IN DATABASE " + escapedCatalog
					} else {
						query += " IN SCHEMA " + escapedCatalog + "." + quoteTblName(*dbSchema)
					}
				}
			default:
				return fmt.Errorf("unimplemented object type")
			}

			*outQueryID, err = getQueryID(ctx, query, driverConn)
			return
		})
	})
}

func isWildcardStr(ident string) bool {
	return strings.ContainsAny(ident, "_%")
}

// DAVIDCOE --------------------------------------

type Metadata struct {
	Created                                                                                                                      time.Time
	ColName, DataType, Dbname, Kind, Schema, TblName, TblType, IdentGen, IdentIncrement, Comment, ConstraintName, ConstraintType sql.NullString
	OrdinalPos                                                                                                                   int
	NumericPrec, NumericPrecRadix, NumericScale, DatetimePrec                                                                    sql.NullInt16
	IsNullable, IsIdent                                                                                                          bool
	CharMaxLength, CharOctetLength                                                                                               sql.NullInt32
}

type ConstraintSchema struct {
	ConstraintName, ConstraintType string
	ConstraintColumnNames          []string
	ConstraintColumnUsages         []internal.UsageSchema
}

type QualifiedConstraint struct {
	catalogSchemaTable internal.CatalogSchemaTable
	constraintName     string
}

type TableConstraint struct {
	dbName, schema, tblName, colName, constraintName, constraintType string
	fkDbName, fkSchema, fkTblName, fkColName, fkConstraintName       sql.NullString
	skipUpdateRule, skipDeleteRule, skipDeferrability                string
	keySequence                                                      int
	skipComment                                                      sql.NullString
	skipCreatedOn                                                    time.Time
	skipRely                                                         bool
}

func (c *connectionImpl) GetObjectsOrig(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	metadataRecords, err := c.populateMetadata(ctx, depth, catalog, dbSchema, tableName, columnName, tableType)
	if err != nil {
		return nil, err
	}

	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	g.MetadataRecords = metadataRecords

	constraintLookup, err := c.populateConstraintSchema(ctx, depth, metadataRecords)
	g.ConstraintLookup = constraintLookup
	if err != nil {
		return nil, err
	}

	if err := g.Init(c.db.Alloc, c.getObjectsDbSchemas, c.getObjectsTables); err != nil {
		return nil, err
	}
	defer g.Release()

	uniqueCatalogs := make(map[string]bool)
	for _, data := range metadataRecords {
		if !data.Dbname.Valid {
			continue
		}

		if _, exists := uniqueCatalogs[data.Dbname.String]; !exists {
			uniqueCatalogs[data.Dbname.String] = true
			g.AppendCatalog(data.Dbname.String)
		}
	}

	return g.Finish()
}

func (c *connectionImpl) getObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, metadataRecords []internal.Metadata) (result map[string][]string, err error) {
	if depth == adbc.ObjectDepthCatalogs {
		return
	}

	result = make(map[string][]string)
	uniqueCatalogSchema := make(map[internal.CatalogAndSchema]bool)

	for _, data := range metadataRecords {
		if !data.Dbname.Valid || !data.Schema.Valid {
			continue
		}

		catalogSchemaInfo := internal.CatalogAndSchema{
			Catalog: data.Dbname.String,
			Schema:  data.Schema.String,
		}

		cat, exists := result[data.Dbname.String]
		if !exists {
			cat = make([]string, 0, 1)
		}

		if _, exists := uniqueCatalogSchema[catalogSchemaInfo]; !exists {
			uniqueCatalogSchema[catalogSchemaInfo] = true
			result[data.Dbname.String] = append(cat, data.Schema.String)
		}
	}

	return
}

func toField(name string, isnullable bool, dataType string, numPrec, numPrecRadix, numScale sql.NullInt16, isIdent, useHighPrecision bool, identGen, identInc sql.NullString, charMaxLength, charOctetLength sql.NullInt32, datetimePrec sql.NullInt16, comment sql.NullString, ordinalPos int) (ret arrow.Field) {
	ret.Name, ret.Nullable = name, isnullable

	switch dataType {
	case "NUMBER":
		if useHighPrecision {
			ret.Type = &arrow.Decimal128Type{
				Precision: int32(numPrec.Int16),
				Scale:     int32(numScale.Int16),
			}
		} else {
			if !numScale.Valid || numScale.Int16 == 0 {
				ret.Type = arrow.PrimitiveTypes.Int64
			} else {
				ret.Type = arrow.PrimitiveTypes.Float64
			}
		}
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		ret.Type = arrow.PrimitiveTypes.Float64
	case "TEXT":
		ret.Type = arrow.BinaryTypes.String
	case "BINARY":
		ret.Type = arrow.BinaryTypes.Binary
	case "BOOLEAN":
		ret.Type = arrow.FixedWidthTypes.Boolean
	case "ARRAY":
		fallthrough
	case "VARIANT":
		fallthrough
	case "OBJECT":
		// snowflake will return each value as a string
		ret.Type = arrow.BinaryTypes.String
	case "DATE":
		ret.Type = arrow.FixedWidthTypes.Date32
	case "TIME":
		ret.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		ret.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMP_LTZ":
		ret.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
	case "TIMESTAMP_TZ":
		ret.Type = arrow.FixedWidthTypes.Timestamp_ns
	case "GEOGRAPHY":
		fallthrough
	case "GEOMETRY":
		ret.Type = arrow.BinaryTypes.String
	}

	md := make(map[string]string)
	md["TYPE_NAME"] = dataType
	if isIdent {
		md["IS_IDENTITY"] = "YES"
		md["IDENTITY_GENERATION"] = identGen.String
		md["IDENTITY_INCREMENT"] = identInc.String
	}
	if comment.Valid {
		md["COMMENT"] = comment.String
	}

	md["ORDINAL_POSITION"] = strconv.Itoa(ordinalPos)
	md["XDBC_DATA_TYPE"] = strconv.Itoa(int(ret.Type.ID()))
	md["XDBC_TYPE_NAME"] = dataType
	md["XDBC_SQL_DATA_TYPE"] = strconv.Itoa(int(toXdbcDataType(ret.Type)))
	md["XDBC_NULLABLE"] = strconv.FormatBool(isnullable)

	if isnullable {
		md["XDBC_IS_NULLABLE"] = "YES"
	} else {
		md["XDBC_IS_NULLABLE"] = "NO"
	}

	if numPrec.Valid {
		md["XDBC_PRECISION"] = strconv.Itoa(int(numPrec.Int16))
	}

	if numScale.Valid {
		md["XDBC_SCALE"] = strconv.Itoa(int(numScale.Int16))
	}

	if numPrec.Valid {
		md["XDBC_PRECISION"] = strconv.Itoa(int(numPrec.Int16))
	}

	if numPrecRadix.Valid {
		md["XDBC_NUM_PREC_RADIX"] = strconv.Itoa(int(numPrecRadix.Int16))
	}

	if charMaxLength.Valid {
		md["CHARACTER_MAXIMUM_LENGTH"] = strconv.Itoa(int(charMaxLength.Int32))
	}

	if charOctetLength.Valid {
		md["XDBC_CHAR_OCTET_LENGTH"] = strconv.Itoa(int(charOctetLength.Int32))
	}

	if datetimePrec.Valid {
		md["XDBC_DATETIME_SUB"] = strconv.Itoa(int(datetimePrec.Int16))
	}

	ret.Metadata = arrow.MetadataFrom(md)

	return
}

func toXdbcDataType(dt arrow.DataType) (xdbcType internal.XdbcDataType) {
	switch dt.ID() {
	case arrow.EXTENSION:
		return toXdbcDataType(dt.(arrow.ExtensionType).StorageType())
	case arrow.DICTIONARY:
		return toXdbcDataType(dt.(*arrow.DictionaryType).ValueType)
	case arrow.RUN_END_ENCODED:
		return toXdbcDataType(dt.(*arrow.RunEndEncodedType).Encoded())
	case arrow.INT8, arrow.UINT8:
		return internal.XdbcDataType_XDBC_TINYINT
	case arrow.INT16, arrow.UINT16:
		return internal.XdbcDataType_XDBC_SMALLINT
	case arrow.INT32, arrow.UINT32:
		return internal.XdbcDataType_XDBC_SMALLINT
	case arrow.INT64, arrow.UINT64:
		return internal.XdbcDataType_XDBC_BIGINT
	case arrow.FLOAT32, arrow.FLOAT16, arrow.FLOAT64:
		return internal.XdbcDataType_XDBC_FLOAT
	case arrow.DECIMAL, arrow.DECIMAL256:
		return internal.XdbcDataType_XDBC_DECIMAL
	case arrow.STRING, arrow.LARGE_STRING:
		return internal.XdbcDataType_XDBC_VARCHAR
	case arrow.BINARY, arrow.LARGE_BINARY:
		return internal.XdbcDataType_XDBC_BINARY
	case arrow.FIXED_SIZE_BINARY:
		return internal.XdbcDataType_XDBC_BINARY
	case arrow.BOOL:
		return internal.XdbcDataType_XDBC_BIT
	case arrow.TIME32, arrow.TIME64:
		return internal.XdbcDataType_XDBC_TIME
	case arrow.DATE32, arrow.DATE64:
		return internal.XdbcDataType_XDBC_DATE
	case arrow.TIMESTAMP:
		return internal.XdbcDataType_XDBC_TIMESTAMP
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return internal.XdbcDataType_XDBC_VARBINARY
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return internal.XdbcDataType_XDBC_VARBINARY
	case arrow.STRUCT, arrow.MAP:
		return internal.XdbcDataType_XDBC_VARBINARY
	default:
		return internal.XdbcDataType_XDBC_UNKNOWN_TYPE
	}
}

func (c *connectionImpl) getObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (result internal.SchemaToTableInfo, err error) {

	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas {
		return
	}

	result = make(internal.SchemaToTableInfo)
	includeSchema := depth == adbc.ObjectDepthAll || depth == adbc.ObjectDepthColumns

	uniqueCatalogSchemaTable := make(map[internal.CatalogSchemaTable]bool)
	for _, data := range metadataRecords {
		if !data.Dbname.Valid || !data.Schema.Valid || !data.TblName.Valid || !data.TblType.Valid {
			continue
		}

		catalogSchemaTableInfo := internal.CatalogSchemaTable{
			Catalog: data.Dbname.String,
			Schema:  data.Schema.String,
			Table:   data.TblName.String,
		}

		if _, exists := uniqueCatalogSchemaTable[catalogSchemaTableInfo]; !exists {
			uniqueCatalogSchemaTable[catalogSchemaTableInfo] = true

			key := internal.CatalogAndSchema{
				Catalog: data.Dbname.String, Schema: data.Schema.String}

			result[key] = append(result[key], internal.TableInfo{
				Name: data.TblName.String, TableType: data.TblType.String})
		}
	}

	if includeSchema {
		var (
			prevKey      internal.CatalogAndSchema
			curTableInfo *internal.TableInfo
			fieldList    = make([]arrow.Field, 0)
		)

		uniqueColumn := make(map[internal.CatalogSchemaTableColumn]bool)

		for _, data := range metadataRecords {
			if !data.Dbname.Valid || !data.Schema.Valid || !data.TblName.Valid || !data.ColName.Valid {
				continue
			}

			key := internal.CatalogAndSchema{Catalog: data.Dbname.String, Schema: data.Schema.String}
			if prevKey != key || (curTableInfo != nil && curTableInfo.Name != data.TblName.String) {
				if len(fieldList) > 0 && curTableInfo != nil {
					curTableInfo.Schema = arrow.NewSchema(fieldList, nil)
					fieldList = fieldList[:0]
				}

				info := result[key]
				for i := range info {
					if info[i].Name == data.TblName.String {
						curTableInfo = &info[i]
						break
					}
				}
			}

			prevKey = key
			columnInfo := internal.CatalogSchemaTableColumn{
				Catalog: data.Dbname.String,
				Schema:  data.Schema.String,
				Table:   data.TblName.String,
				Column:  data.ColName.String,
			}
			if _, exists := uniqueColumn[columnInfo]; !exists {
				uniqueColumn[columnInfo] = true
				fieldList = append(fieldList, toField(data.ColName.String, data.IsNullable, data.DataType.String, data.NumericPrec, data.NumericPrecRadix, data.NumericScale, data.IsIdent, c.useHighPrecision, data.IdentGen, data.IdentIncrement, data.CharMaxLength, data.CharOctetLength, data.DatetimePrec, data.Comment, data.OrdinalPos))
			}
		}

		if len(fieldList) > 0 && curTableInfo != nil {
			curTableInfo.Schema = arrow.NewSchema(fieldList, nil)
		}
	}
	return
}

func (c *connectionImpl) populateMetadata(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) ([]internal.Metadata, error) {
	var metadataRecords []internal.Metadata

	catalogMetadataRecords, err := c.getCatalogsMetadata(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	matchingCatalogNames, err := getMatchingCatalogNames(catalogMetadataRecords, catalog)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	}

	if depth == adbc.ObjectDepthCatalogs {
		metadataRecords = catalogMetadataRecords
	} else if depth == adbc.ObjectDepthDBSchemas {
		metadataRecords, err = c.getDbSchemasMetadata(ctx, matchingCatalogNames, catalog, dbSchema)

	} else if depth == adbc.ObjectDepthTables {
		metadataRecords, err = c.getTablesMetadata(ctx, matchingCatalogNames, catalog, dbSchema, tableName, tableType)
	} else {
		tableMetadataRecords, tablesErr := c.getTablesMetadata(ctx, matchingCatalogNames, catalog, dbSchema, tableName, tableType)
		if tablesErr != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		columnsMetadataRecords, columnsErr := c.getColumnsMetadata(ctx, matchingCatalogNames, catalog, dbSchema, tableName, columnName, tableType)
		if columnsErr != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		metadataRecords = append(tableMetadataRecords, columnsMetadataRecords...)
	}

	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	return metadataRecords, nil
}

func (c *connectionImpl) populateConstraintSchema(ctx context.Context, depth adbc.ObjectDepth, metadataRecords []internal.Metadata) (map[internal.CatalogSchemaTable][]internal.ConstraintSchema, error) {
	constraintLookup := make(map[internal.CatalogSchemaTable][]internal.ConstraintSchema)
	tableConstraintsData, err := c.getConstraintsData(ctx, depth, metadataRecords)
	if err != nil {
		return nil, err
	}

	// we want to avoid creating duplicate entries for a constraint
	qualifiedConstraintLookup := make(map[QualifiedConstraint]internal.ConstraintSchema)
	for _, data := range tableConstraintsData {
		var qualifiedConstraint QualifiedConstraint
		// columnUsages is only relevant for a foreign key
		if data.fkConstraintName.Valid {
			qualifiedConstraint = getQualifiedConstraint(data.fkDbName.String, data.fkSchema.String, data.fkTblName.String, data.fkConstraintName.String)
			if _, exists := qualifiedConstraintLookup[qualifiedConstraint]; !exists {
				qualifiedConstraintLookup[qualifiedConstraint] = getConstraintSchemaFromTableConstraint(data)
			} else {
				constraintInfo := qualifiedConstraintLookup[qualifiedConstraint]
				// appending additional column names and column usages for foreign key constraints
				constraintInfo.ConstraintColumnNames = append(constraintInfo.ConstraintColumnNames, data.fkColName.String)
				constraintInfo.ConstraintColumnUsages = append(constraintInfo.ConstraintColumnUsages, getUsageSchemaFromTableConstraint(data))
				qualifiedConstraintLookup[qualifiedConstraint] = constraintInfo
			}
		} else {
			qualifiedConstraint = getQualifiedConstraint(data.dbName, data.schema, data.tblName, data.constraintName)
			if _, exists := qualifiedConstraintLookup[qualifiedConstraint]; !exists {
				qualifiedConstraintLookup[qualifiedConstraint] = getConstraintSchemaFromTableConstraint(data)
			} else {
				constraintInfo := qualifiedConstraintLookup[qualifiedConstraint]
				// appending additional column names for primary and unique key constraints
				constraintInfo.ConstraintColumnNames = append(constraintInfo.ConstraintColumnNames, data.colName)
				qualifiedConstraintLookup[qualifiedConstraint] = constraintInfo
			}
		}
	}

	// adding all the unique constraints to a constraint lookup using the catalogSchemaTable as a key
	for qualifiedConstraint, constraintSchema := range qualifiedConstraintLookup {
		catalogSchemaTable := qualifiedConstraint.catalogSchemaTable
		constraintLookup[catalogSchemaTable] = append(constraintLookup[catalogSchemaTable], constraintSchema)
	}

	return constraintLookup, nil
}

func (c *connectionImpl) getConstraintsData(ctx context.Context, depth adbc.ObjectDepth, metadataRecords []internal.Metadata) ([]TableConstraint, error) {
	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas {
		return nil, nil
	}
	availableConstraintTypes := getAvailableConstraintTypes(metadataRecords)
	availableFullyQualifiedConstraints := getAvailableConstraints(metadataRecords)

	var uniqueConstraintsData []TableConstraint
	var primaryKeyConstraintsData []TableConstraint
	var foreignKeyConstraintsData []TableConstraint
	var err error

	if availableConstraintTypes != nil {
		if _, exists := availableConstraintTypes[internal.Unique]; exists {
			uniqueConstraintsData, err = c.getUniqueConstraints(ctx, availableFullyQualifiedConstraints)
			if err != nil {
				return nil, errToAdbcErr(adbc.StatusIO, err)
			}
		}

		if _, exists := availableConstraintTypes[internal.PrimaryKey]; exists {
			primaryKeyConstraintsData, err = c.getPrimaryKeyConstraints(ctx, availableFullyQualifiedConstraints)
			if err != nil {
				return nil, errToAdbcErr(adbc.StatusIO, err)
			}
		}

		if _, exists := availableConstraintTypes[internal.ForeignKey]; exists {
			foreignKeyConstraintsData, err = c.getForeignKeyConstraints(ctx, availableFullyQualifiedConstraints)
			if err != nil {
				return nil, errToAdbcErr(adbc.StatusIO, err)
			}
		}
	}

	tableConstraintsData := append(append(uniqueConstraintsData, primaryKeyConstraintsData...), foreignKeyConstraintsData...)

	slices.SortFunc(tableConstraintsData, func(i, j TableConstraint) int {
		if n := cmp.Compare(i.constraintName, j.constraintName); n != 0 {
			return n
		}
		// If constrain names are equal, order by keySequence
		return cmp.Compare(i.keySequence, j.keySequence)
	})

	return tableConstraintsData, nil
}

func (c *connectionImpl) getUniqueConstraints(ctx context.Context, fullyQualifiedConstraints map[QualifiedConstraint]bool) ([]TableConstraint, error) {
	uniqueConstraintsData := make([]TableConstraint, 0)

	rows, err := c.sqldb.QueryContext(ctx, prepareUniqueConstraintSQL(), nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	for rows.Next() {
		var uniqueConstraint TableConstraint
		if err := rows.Scan(&uniqueConstraint.skipCreatedOn, &uniqueConstraint.dbName, &uniqueConstraint.schema,
			&uniqueConstraint.tblName, &uniqueConstraint.colName, &uniqueConstraint.keySequence,
			&uniqueConstraint.constraintName, &uniqueConstraint.skipRely, &uniqueConstraint.skipComment); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		currentQualifiedConstraint := getQualifiedConstraint(uniqueConstraint.dbName, uniqueConstraint.schema, uniqueConstraint.tblName, uniqueConstraint.constraintName)

		// skip constraint if it doesn't exist in fullyQualifiedConstraints
		if _, exists := fullyQualifiedConstraints[currentQualifiedConstraint]; exists {
			uniqueConstraint.constraintType = internal.Unique
			uniqueConstraintsData = append(uniqueConstraintsData, uniqueConstraint)
		}

	}
	return uniqueConstraintsData, nil
}

func (c *connectionImpl) getPrimaryKeyConstraints(ctx context.Context, fullyQualifiedConstraints map[QualifiedConstraint]bool) ([]TableConstraint, error) {
	primaryKeyConstraintsData := make([]TableConstraint, 0)

	rows, err := c.sqldb.QueryContext(ctx, preparePrimaryKeyConstraintSQL(), nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	for rows.Next() {
		var primaryKeyConstraint TableConstraint
		if err := rows.Scan(&primaryKeyConstraint.skipCreatedOn, &primaryKeyConstraint.dbName, &primaryKeyConstraint.schema,
			&primaryKeyConstraint.tblName, &primaryKeyConstraint.colName, &primaryKeyConstraint.keySequence,
			&primaryKeyConstraint.constraintName, &primaryKeyConstraint.skipRely, &primaryKeyConstraint.skipComment); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		currentQualifiedConstraint := getQualifiedConstraint(primaryKeyConstraint.dbName, primaryKeyConstraint.schema, primaryKeyConstraint.tblName, primaryKeyConstraint.constraintName)

		// skip constraint if it doesn't exist in fullyQualifiedConstraints
		if _, exists := fullyQualifiedConstraints[currentQualifiedConstraint]; exists {
			primaryKeyConstraint.constraintType = internal.PrimaryKey
			primaryKeyConstraintsData = append(primaryKeyConstraintsData, primaryKeyConstraint)
		}

	}
	return primaryKeyConstraintsData, nil
}

func (c *connectionImpl) getForeignKeyConstraints(ctx context.Context, qualifiedConstraints map[QualifiedConstraint]bool) ([]TableConstraint, error) {
	foreignKeyConstraintsData := make([]TableConstraint, 0)

	rows, err := c.sqldb.QueryContext(ctx, prepareForeignKeyConstraintSQL(), nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	for rows.Next() {
		var fkConstraint TableConstraint
		if err := rows.Scan(&fkConstraint.skipCreatedOn, &fkConstraint.dbName, &fkConstraint.schema,
			&fkConstraint.tblName, &fkConstraint.colName, &fkConstraint.fkDbName, &fkConstraint.fkSchema,
			&fkConstraint.fkTblName, &fkConstraint.fkColName, &fkConstraint.keySequence,
			&fkConstraint.skipUpdateRule, &fkConstraint.skipDeleteRule, &fkConstraint.fkConstraintName,
			&fkConstraint.constraintName, &fkConstraint.skipDeferrability, &fkConstraint.skipRely, &fkConstraint.skipComment); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		currentQualifiedConstaint := getQualifiedConstraint(fkConstraint.fkDbName.String, fkConstraint.fkSchema.String, fkConstraint.fkTblName.String, fkConstraint.fkConstraintName.String)

		// skip constraint if it doesn't exist in qualifiedConstraints
		if _, exists := qualifiedConstraints[currentQualifiedConstaint]; exists {
			fkConstraint.constraintType = internal.ForeignKey
			foreignKeyConstraintsData = append(foreignKeyConstraintsData, fkConstraint)
		}
	}
	return foreignKeyConstraintsData, nil
}

func (c *connectionImpl) getCatalogsMetadata(ctx context.Context) ([]internal.Metadata, error) {
	metadataRecords := make([]internal.Metadata, 0)

	rows, err := c.sqldb.QueryContext(ctx, prepareCatalogsSQL(), nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	for rows.Next() {
		var data internal.Metadata
		var skipDbNullField, skipSchemaNullField sql.NullString
		// schema for SHOW TERSE DATABASES is:
		// created_on:timestamp, name:text, kind:null, database_name:null, schema_name:null
		// the last three columns are always null because they are not applicable for databases
		// so we want values[1].(string) for the name
		if err := rows.Scan(&data.Created, &data.Dbname, &data.Kind, &skipDbNullField, &skipSchemaNullField); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		// SNOWFLAKE catalog contains functions and no tables
		if data.Dbname.Valid && data.Dbname.String == "SNOWFLAKE" {
			continue
		}

		metadataRecords = append(metadataRecords, data)
	}
	return metadataRecords, nil
}

func (c *connectionImpl) getDbSchemasMetadata(ctx context.Context, matchingCatalogNames []string, catalog *string, dbSchema *string) ([]internal.Metadata, error) {
	var metadataRecords []internal.Metadata
	query, queryArgs := prepareDbSchemasSQL(matchingCatalogNames, catalog, dbSchema)
	rows, err := c.sqldb.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	for rows.Next() {
		var data internal.Metadata
		if err = rows.Scan(&data.Dbname, &data.Schema); err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		metadataRecords = append(metadataRecords, data)
	}
	return metadataRecords, nil
}

func (c *connectionImpl) getTablesMetadata(ctx context.Context, matchingCatalogNames []string, catalog *string, dbSchema *string, tableName *string, tableType []string) ([]internal.Metadata, error) {
	metadataRecords := make([]internal.Metadata, 0)
	query, queryArgs := prepareTablesSQL(matchingCatalogNames, catalog, dbSchema, tableName, tableType)
	rows, err := c.sqldb.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	for rows.Next() {
		var data internal.Metadata
		if err = rows.Scan(&data.Dbname, &data.Schema, &data.TblName, &data.TblType, &data.ConstraintName, &data.ConstraintType); err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		metadataRecords = append(metadataRecords, data)
	}
	return metadataRecords, nil
}

func (c *connectionImpl) getColumnsMetadata(ctx context.Context, matchingCatalogNames []string, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) ([]internal.Metadata, error) {
	metadataRecords := make([]internal.Metadata, 0)
	query, queryArgs := prepareColumnsSQL(matchingCatalogNames, catalog, dbSchema, tableName, columnName, tableType)
	rows, err := c.sqldb.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	var data internal.Metadata

	for rows.Next() {
		// order here matches the order of the columns requested in the query
		err = rows.Scan(&data.Dbname, &data.Schema, &data.TblName, &data.ColName,
			&data.OrdinalPos, &data.IsNullable, &data.DataType, &data.NumericPrec,
			&data.NumericPrecRadix, &data.NumericScale, &data.IsIdent, &data.IdentGen,
			&data.IdentIncrement, &data.CharMaxLength, &data.CharOctetLength, &data.DatetimePrec, &data.Comment)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		metadataRecords = append(metadataRecords, data)
	}
	return metadataRecords, nil
}

func getAvailableConstraintTypes(metadataRecords []internal.Metadata) map[string]bool {
	availableConstraintType := make(map[string]bool)
	for _, data := range metadataRecords {
		if data.ConstraintType.Valid {
			switch data.ConstraintType.String {
			case internal.Unique:
				availableConstraintType[internal.Unique] = true
			case internal.PrimaryKey:
				availableConstraintType[internal.PrimaryKey] = true
			case internal.ForeignKey:
				availableConstraintType[internal.ForeignKey] = true
			default:
			}
		}
	}
	return availableConstraintType
}

func getAvailableConstraints(metadataRecords []internal.Metadata) map[QualifiedConstraint]bool {
	qualifiedConstraints := make(map[QualifiedConstraint]bool)
	for _, data := range metadataRecords {
		if data.ConstraintName.Valid {
			qualifiedConstraint := getQualifiedConstraint(data.Dbname.String, data.Schema.String, data.TblName.String, data.ConstraintName.String)
			qualifiedConstraints[qualifiedConstraint] = true
		}
	}
	return qualifiedConstraints
}

func getQualifiedConstraint(dbName string, schema string, tableName string, constraintName string) QualifiedConstraint {
	return QualifiedConstraint{
		catalogSchemaTable: internal.CatalogSchemaTable{
			Catalog: dbName,
			Schema:  schema,
			Table:   tableName,
		},
		constraintName: constraintName,
	}
}

func getConstraintSchemaFromTableConstraint(tableConstraint TableConstraint) internal.ConstraintSchema {
	var constraintSchema internal.ConstraintSchema
	constraintSchema.ConstraintType = tableConstraint.constraintType

	if tableConstraint.fkConstraintName.Valid {
		usageSchema := getUsageSchemaFromTableConstraint(tableConstraint)
		constraintSchema.ConstraintName = tableConstraint.fkConstraintName.String
		constraintSchema.ConstraintColumnNames = []string{tableConstraint.fkColName.String}
		constraintSchema.ConstraintColumnUsages = []internal.UsageSchema{usageSchema}
	} else {
		constraintSchema.ConstraintName = tableConstraint.constraintName
		constraintSchema.ConstraintColumnNames = []string{tableConstraint.colName}
	}
	return constraintSchema
}

func getUsageSchemaFromTableConstraint(tableConstraint TableConstraint) internal.UsageSchema {
	var usageSchema internal.UsageSchema
	// usageSchema is only applicable for foreign key constraint
	if tableConstraint.fkConstraintName.Valid {
		// reference column for a foreign key constraint
		usageSchema = internal.UsageSchema{
			ForeignKeyCatalog:  tableConstraint.dbName,
			ForeignKeyDbSchema: tableConstraint.schema,
			ForeignKeyTable:    tableConstraint.tblName,
			ForeignKeyColName:  tableConstraint.colName,
		}
	}
	return usageSchema
}

func getMatchingCatalogNames(metadataRecords []internal.Metadata, catalog *string) ([]string, error) {
	matchingCatalogNames := make([]string, 0)
	var catalogPattern *regexp.Regexp
	var err error
	if catalogPattern, err = internal.PatternToRegexp(catalog); err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	}

	for _, data := range metadataRecords {
		if data.Dbname.Valid && data.Dbname.String == "SNOWFLAKE" {
			continue
		}
		if catalogPattern != nil && !catalogPattern.MatchString(data.Dbname.String) {
			continue
		}

		matchingCatalogNames = append(matchingCatalogNames, data.Dbname.String)
	}
	return matchingCatalogNames, nil
}

func prepareCatalogsSQL() string {
	return "SHOW TERSE DATABASES"
}

func prepareDbSchemasSQL(matchingCatalogNames []string, catalog *string, dbSchema *string) (string, []interface{}) {
	query := ""
	for _, catalog_name := range matchingCatalogNames {
		if query != "" {
			query += " UNION ALL "
		}
		query += `SELECT * FROM "` + strings.ReplaceAll(catalog_name, "\"", "\"\"") + `".INFORMATION_SCHEMA.SCHEMATA`
	}

	query = `SELECT CATALOG_NAME, SCHEMA_NAME FROM (` + query + `)`
	conditions, queryArgs := prepareFilterConditions(adbc.ObjectDepthDBSchemas, catalog, dbSchema, nil, nil, make([]string, 0))
	if conditions != "" {
		query += " WHERE " + conditions
	}

	return query, queryArgs
}

func prepareTablesSQL(matchingCatalogNames []string, catalog *string, dbSchema *string, tableName *string, tableType []string) (string, []interface{}) {
	query := ""
	for _, catalog_name := range matchingCatalogNames {
		if query != "" {
			query += " UNION ALL "
		}
		query += `SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
					FROM
					(
						"` + strings.ReplaceAll(catalog_name, "\"", "\"\"") + `".INFORMATION_SCHEMA.TABLES AS T
						LEFT JOIN
						"` + strings.ReplaceAll(catalog_name, "\"", "\"\"") + `".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
						ON
							T.table_catalog = TC.table_catalog
							AND T.table_schema = TC.table_schema
							AND t.table_name = TC.table_name
					)`
	}

	query = `SELECT table_catalog, table_schema, table_name, table_type, constraint_name, constraint_type FROM (` + query + `)`
	conditions, queryArgs := prepareFilterConditions(adbc.ObjectDepthTables, catalog, dbSchema, tableName, nil, tableType)
	if conditions != "" {
		query += " WHERE " + conditions
	}
	return query, queryArgs
}

func prepareColumnsSQL(matchingCatalogNames []string, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (string, []interface{}) {
	prefixQuery := ""
	for _, catalogName := range matchingCatalogNames {
		if prefixQuery != "" {
			prefixQuery += " UNION ALL "
		}
		prefixQuery += `SELECT * FROM "` + strings.ReplaceAll(catalogName, "\"", "\"\"") + `".INFORMATION_SCHEMA.COLUMNS`
	}

	prefixQuery = `SELECT table_catalog, table_schema, table_name, column_name,
						ordinal_position, is_nullable::boolean, data_type, numeric_precision,
						numeric_precision_radix, numeric_scale, is_identity::boolean,
						identity_generation, identity_increment,
						character_maximum_length, character_octet_length, datetime_precision, comment FROM (` + prefixQuery + `)`
	ordering := ` ORDER BY table_catalog, table_schema, table_name, ordinal_position`
	conditions, queryArgs := prepareFilterConditions(adbc.ObjectDepthColumns, catalog, dbSchema, tableName, columnName, tableType)
	query := prefixQuery

	if conditions != "" {
		query += " WHERE " + conditions
	}

	query += ordering
	return query, queryArgs
}

func prepareFilterConditions(depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (string, []interface{}) {
	conditions := make([]string, 0)
	queryArgs := make([]interface{}, 0)
	if catalog != nil && *catalog != "" {
		if depth == adbc.ObjectDepthDBSchemas {
			conditions = append(conditions, ` CATALOG_NAME ILIKE ? `)
		} else {
			conditions = append(conditions, ` TABLE_CATALOG ILIKE ? `)
		}
		queryArgs = append(queryArgs, *catalog)
	}
	if dbSchema != nil && *dbSchema != "" {
		if depth == adbc.ObjectDepthDBSchemas {
			conditions = append(conditions, ` SCHEMA_NAME ILIKE ? `)
		} else {
			conditions = append(conditions, ` TABLE_SCHEMA ILIKE ? `)
		}
		queryArgs = append(queryArgs, *dbSchema)
	}
	if tableName != nil && *tableName != "" {
		conditions = append(conditions, ` TABLE_NAME ILIKE ? `)
		queryArgs = append(queryArgs, *tableName)
	}
	if columnName != nil && *columnName != "" {
		conditions = append(conditions, ` COLUMN_NAME ILIKE ? `)
		queryArgs = append(queryArgs, *columnName)
	}

	var tblConditions []string
	if len(tableType) > 0 && depth == adbc.ObjectDepthTables {
		tblConditions = append(conditions, ` TABLE_TYPE IN ('`+strings.Join(tableType, `','`)+`')`)
	} else {
		tblConditions = conditions
	}

	cond := strings.Join(tblConditions, " AND ")
	return cond, queryArgs
}

func prepareUniqueConstraintSQL() string {
	return "SHOW UNIQUE KEYS"
}

func preparePrimaryKeyConstraintSQL() string {
	return "SHOW PRIMARY KEYS"
}

func prepareForeignKeyConstraintSQL() string {
	return "SHOW EXPORTED KEYS"
}

// DAVIDCOE -------------------------------------

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	useGetObjectsOrig := os.Getenv("SNOWFLAKE_GET_OBJECTS_ORIG")
	if (depth == adbc.ObjectDepthAll || depth == adbc.ObjectDepthColumns) && useGetObjectsOrig == "true" {
		return c.GetObjectsOrig(ctx, depth, catalog, dbSchema, tableName, columnName, tableType)
	}

	var (
		pkQueryID, fkQueryID, uniqueQueryID, terseDbQueryID string
		showSchemaQueryID, tableQueryID                     string
	)

	conn, err := c.sqldb.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var hasViews, hasTables bool
	for _, t := range tableType {
		if strings.EqualFold("VIEW", t) {
			hasViews = true
		} else if strings.EqualFold("TABLE", t) {
			hasTables = true
		}
	}

	// force empty result from SHOW TABLES if tableType list is not empty
	// and does not contain TABLE or VIEW in the list.
	// we need this because we should have non-null db_schema_tables when
	// depth is Tables, Columns or All.
	var badTableType = "tabletypedoesnotexist"
	if len(tableType) > 0 && depth >= adbc.ObjectDepthTables && !hasViews && !hasTables {
		tableName = &badTableType
		tableType = []string{"TABLE"}
	}

	gQueryIDs, gQueryIDsCtx := errgroup.WithContext(ctx)
	queryFile := queryTemplateGetObjectsAll
	switch depth {
	case adbc.ObjectDepthCatalogs:
		queryFile = queryTemplateGetObjectsTerseCatalogs
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)
	case adbc.ObjectDepthDBSchemas:
		queryFile = queryTemplateGetObjectsDbSchemas
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, &showSchemaQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)
	case adbc.ObjectDepthTables:
		queryFile = queryTemplateGetObjectsTables
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, &showSchemaQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)

		objType := objObjects
		if len(tableType) == 1 {
			if strings.EqualFold("VIEW", tableType[0]) {
				objType = objViews
			} else if strings.EqualFold("TABLE", tableType[0]) {
				objType = objTables
			}
		}

		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objType,
			catalog, dbSchema, tableName, &tableQueryID)
	default:
		var suffix string
		if catalog == nil || isWildcardStr(*catalog) {
			suffix = " IN ACCOUNT"
		} else {
			escapedCatalog := quoteTblName(*catalog)
			if dbSchema == nil || isWildcardStr(*dbSchema) {
				suffix = " IN DATABASE " + escapedCatalog
			} else {
				escapedSchema := quoteTblName(*dbSchema)
				if tableName == nil || isWildcardStr(*tableName) {
					suffix = " IN SCHEMA " + escapedCatalog + "." + escapedSchema
				} else {
					escapedTable := quoteTblName(*tableName)
					suffix = " IN TABLE " + escapedCatalog + "." + escapedSchema + "." + escapedTable
				}
			}

			// Detailed constraint info not available in information_schema
			// Need to dispatch SHOW queries and use conn.Raw to extract the queryID for reuse in GetObjects query
			gQueryIDs.Go(func() error {
				return conn.Raw(func(driverConn any) (err error) {
					pkQueryID, err = getQueryID(gQueryIDsCtx, "SHOW PRIMARY KEYS /* ADBC:getObjectsTables */"+suffix, driverConn)
					return err
				})
			})

			gQueryIDs.Go(func() error {
				return conn.Raw(func(driverConn any) (err error) {
					fkQueryID, err = getQueryID(gQueryIDsCtx, "SHOW IMPORTED KEYS /* ADBC:getObjectsTables */"+suffix, driverConn)
					return err
				})
			})

			gQueryIDs.Go(func() error {
				return conn.Raw(func(driverConn any) (err error) {
					uniqueQueryID, err = getQueryID(gQueryIDsCtx, "SHOW UNIQUE KEYS /* ADBC:getObjectsTables */"+suffix, driverConn)
					return err
				})
			})

			goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
				catalog, dbSchema, tableName, &terseDbQueryID)
			goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
				catalog, dbSchema, tableName, &showSchemaQueryID)

			objType := objObjects
			if len(tableType) == 1 {
				if strings.EqualFold("VIEW", tableType[0]) {
					objType = objViews
				} else if strings.EqualFold("TABLE", tableType[0]) {
					objType = objTables
				}
			}
			goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objType,
				catalog, dbSchema, tableName, &tableQueryID)
		}
	}

	queryBytes, err := fs.ReadFile(queryTemplates, path.Join("queries", queryFile))
	if err != nil {
		return nil, err
	}

	// Need constraint subqueries to complete before we can query GetObjects
	if err := gQueryIDs.Wait(); err != nil {
		return nil, err
	}

	args := []any{
		// Optional filter patterns
		driverbase.PatternToNamedArg("CATALOG", catalog),
		driverbase.PatternToNamedArg("DB_SCHEMA", dbSchema),
		driverbase.PatternToNamedArg("TABLE", tableName),
		driverbase.PatternToNamedArg("COLUMN", columnName),

		// QueryIDs for constraint data if depth is tables or deeper
		// or if the depth is catalog and catalog is null
		sql.Named("PK_QUERY_ID", pkQueryID),
		sql.Named("FK_QUERY_ID", fkQueryID),
		sql.Named("UNIQUE_QUERY_ID", uniqueQueryID),
		sql.Named("SHOW_DB_QUERY_ID", terseDbQueryID),
		sql.Named("SHOW_SCHEMA_QUERY_ID", showSchemaQueryID),
		sql.Named("SHOW_TABLE_QUERY_ID", tableQueryID),
	}

	// currently only the Columns / all case still requires a current database/schema
	// to be propagated. The rest of the cases all solely use SHOW queries for the metadata
	// just as done by the snowflake JDBC driver. In those cases we don't need to propagate
	// the current session database/schema.
	if depth == adbc.ObjectDepthColumns || depth == adbc.ObjectDepthAll {
		dbname, err := c.GetCurrentCatalog()
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		schemaname, err := c.GetCurrentDbSchema()
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		// the connection that is used is not the same connection context where the database may have been set
		// if the caller called SetCurrentCatalog() so need to ensure the database context is appropriate
		multiCtx, _ := gosnowflake.WithMultiStatement(ctx, 2)
		_, err = conn.ExecContext(multiCtx, fmt.Sprintf("USE DATABASE %s; USE SCHEMA %s;", quoteTblName(dbname), quoteTblName(schemaname)))
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
	}

	query := string(queryBytes)
	rows, err := conn.QueryContext(ctx, query, args...)

	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	catalogCh := make(chan driverbase.GetObjectsInfo, runtime.NumCPU())
	errCh := make(chan error)

	go func() {
		defer close(catalogCh)
		for rows.Next() {
			var getObjectsCatalog driverbase.GetObjectsInfo
			if err := rows.Scan(&getObjectsCatalog); err != nil {
				errCh <- errToAdbcErr(adbc.StatusInvalidData, err)
				return
			}

			// A few columns need additional processing outside of Snowflake
			for i, sch := range getObjectsCatalog.CatalogDbSchemas {
				for j, tab := range sch.DbSchemaTables {
					for k, col := range tab.TableColumns {
						field := c.toArrowField(col)
						xdbcDataType := driverbase.ToXdbcDataType(field.Type)

						getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcDataType = driverbase.Nullable(int16(field.Type.ID()))
						getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcSqlDataType = driverbase.Nullable(int16(xdbcDataType))
					}
				}
			}

			catalogCh <- getObjectsCatalog
		}
	}()

	return driverbase.BuildGetObjectsRecordReader(c.Alloc, catalogCh, errCh)
}

// PrepareDriverInfo implements driverbase.DriverInfoPreparer.
func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.ConnectionImplBase.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	return c.ConnectionImplBase.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false)
}

// ListTableTypes implements driverbase.TableTypeLister.
func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW"}, nil
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.getStringQuery("SELECT CURRENT_DATABASE()")
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.getStringQuery("SELECT CURRENT_SCHEMA()")
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	_, err := c.cn.ExecContext(context.Background(), fmt.Sprintf("USE DATABASE %s;", quoteTblName(value)), nil)
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	_, err := c.cn.ExecContext(context.Background(), fmt.Sprintf("USE SCHEMA %s;", quoteTblName(value)), nil)
	return err
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled {
		if c.activeTransaction {
			_, err := c.cn.ExecContext(context.Background(), "COMMIT", nil)
			if err != nil {
				return errToAdbcErr(adbc.StatusInternal, err)
			}
			c.activeTransaction = false
		}
		_, err := c.cn.ExecContext(context.Background(), "ALTER SESSION SET AUTOCOMMIT = true", nil)
		return err
	}

	if !c.activeTransaction {
		_, err := c.cn.ExecContext(context.Background(), "BEGIN", nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
		c.activeTransaction = true
	}
	_, err := c.cn.ExecContext(context.Background(), "ALTER SESSION SET AUTOCOMMIT = false", nil)
	return err
}

var loc = time.Now().Location()

func (c *connectionImpl) toArrowField(columnInfo driverbase.ColumnInfo) arrow.Field {
	field := arrow.Field{Name: columnInfo.ColumnName, Nullable: driverbase.ValueOrZero(columnInfo.XdbcNullable) != 0}

	switch driverbase.ValueOrZero(columnInfo.XdbcTypeName) {
	case "NUMBER":
		if c.useHighPrecision {
			field.Type = &arrow.Decimal128Type{
				Precision: int32(driverbase.ValueOrZero(columnInfo.XdbcColumnSize)),
				Scale:     int32(driverbase.ValueOrZero(columnInfo.XdbcDecimalDigits)),
			}
		} else {
			if driverbase.ValueOrZero(columnInfo.XdbcDecimalDigits) == 0 {
				field.Type = arrow.PrimitiveTypes.Int64
			} else {
				field.Type = arrow.PrimitiveTypes.Float64
			}
		}
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		field.Type = arrow.PrimitiveTypes.Float64
	case "TEXT":
		field.Type = arrow.BinaryTypes.String
	case "BINARY":
		field.Type = arrow.BinaryTypes.Binary
	case "BOOLEAN":
		field.Type = arrow.FixedWidthTypes.Boolean
	case "ARRAY":
		fallthrough
	case "VARIANT":
		fallthrough
	case "OBJECT":
		// snowflake will return each value as a string
		field.Type = arrow.BinaryTypes.String
	case "DATE":
		field.Type = arrow.FixedWidthTypes.Date32
	case "TIME":
		field.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMP_LTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
	case "TIMESTAMP_TZ":
		field.Type = arrow.FixedWidthTypes.Timestamp_ns
	case "GEOGRAPHY":
		fallthrough
	case "GEOMETRY":
		field.Type = arrow.BinaryTypes.String
	}

	return field
}

func descToField(name, typ, isnull, primary string, comment sql.NullString) (field arrow.Field, err error) {
	field.Name = strings.ToLower(name)
	if isnull == "Y" {
		field.Nullable = true
	}
	md := make(map[string]string)
	md["DATA_TYPE"] = typ
	md["PRIMARY_KEY"] = primary
	if comment.Valid {
		md["COMMENT"] = comment.String
	}
	field.Metadata = arrow.MetadataFrom(md)

	paren := strings.Index(typ, "(")
	if paren == -1 {
		// types without params
		switch typ {
		case "FLOAT":
			fallthrough
		case "DOUBLE":
			field.Type = arrow.PrimitiveTypes.Float64
		case "DATE":
			field.Type = arrow.FixedWidthTypes.Date32
		// array, object and variant are all represented as strings by
		// snowflake's return
		case "ARRAY":
			fallthrough
		case "OBJECT":
			fallthrough
		case "VARIANT":
			field.Type = arrow.BinaryTypes.String
		case "GEOGRAPHY":
			fallthrough
		case "GEOMETRY":
			field.Type = arrow.BinaryTypes.String
		case "BOOLEAN":
			field.Type = arrow.FixedWidthTypes.Boolean
		default:
			err = adbc.Error{
				Msg:  fmt.Sprintf("Snowflake Data Type %s not implemented", typ),
				Code: adbc.StatusNotImplemented,
			}
		}
		return
	}

	prefix := typ[:paren]
	switch prefix {
	case "VARCHAR", "TEXT":
		field.Type = arrow.BinaryTypes.String
	case "BINARY", "VARBINARY":
		field.Type = arrow.BinaryTypes.Binary
	case "NUMBER":
		comma := strings.Index(typ, ",")
		scale, err := strconv.ParseInt(typ[comma+1:len(typ)-1], 10, 32)
		if err != nil {
			return field, adbc.Error{
				Msg:  "could not parse Scale from type '" + typ + "'",
				Code: adbc.StatusInvalidData,
			}
		}
		if scale == 0 {
			field.Type = arrow.PrimitiveTypes.Int64
		} else {
			field.Type = arrow.PrimitiveTypes.Float64
		}
	case "TIME":
		field.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMP_LTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
	case "TIMESTAMP_TZ":
		field.Type = arrow.FixedWidthTypes.Timestamp_ns
	default:
		err = adbc.Error{
			Msg:  fmt.Sprintf("Snowflake Data Type %s not implemented", typ),
			Code: adbc.StatusNotImplemented,
		}
	}
	return
}

func (c *connectionImpl) getStringQuery(query string) (string, error) {
	result, err := c.cn.QueryContext(context.Background(), query, nil)
	if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}
	defer result.Close()

	if len(result.Columns()) != 1 {
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] Internal query returned wrong number of columns: %s", result.Columns()),
			Code: adbc.StatusInternal,
		}
	}

	dest := make([]driver.Value, 1)
	err = result.Next(dest)
	if err == io.EOF {
		return "", adbc.Error{
			Msg:  "[Snowflake] Internal query returned no rows",
			Code: adbc.StatusInternal,
		}
	} else if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}

	value, ok := dest[0].(string)
	if !ok {
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] Internal query returned wrong type of value: %s", dest[0]),
			Code: adbc.StatusInternal,
		}
	}

	return value, nil
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	tblParts := make([]string, 0, 3)
	if catalog != nil {
		tblParts = append(tblParts, quoteTblName(*catalog))
	}
	if dbSchema != nil {
		tblParts = append(tblParts, quoteTblName(*dbSchema))
	}
	tblParts = append(tblParts, quoteTblName(tableName))
	fullyQualifiedTable := strings.Join(tblParts, ".")

	rows, err := c.sqldb.QueryContext(ctx, `DESC TABLE `+fullyQualifiedTable)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	var (
		name, typ, kind, isnull, primary, unique          string
		def, check, expr, comment, policyName, privDomain sql.NullString
		fields                                            = []arrow.Field{}
	)

	for rows.Next() {
		err := rows.Scan(&name, &typ, &kind, &isnull, &def, &primary, &unique,
			&check, &expr, &comment, &policyName, &privDomain)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		f, err := descToField(name, typ, isnull, primary, comment)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	sc := arrow.NewSchema(fields, nil)
	return sc, nil
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Commit(_ context.Context) error {
	_, err := c.cn.ExecContext(context.Background(), "COMMIT", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Rollback(_ context.Context) error {
	_, err := c.cn.ExecContext(context.Background(), "ROLLBACK", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	defaultIngestOptions := DefaultIngestOptions()
	return &statement{
		alloc:               c.db.Alloc,
		cnxn:                c,
		queueSize:           defaultStatementQueueSize,
		prefetchConcurrency: defaultPrefetchConcurrency,
		useHighPrecision:    c.useHighPrecision,
		ingestOptions:       defaultIngestOptions,
	}, nil
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close() error {
	if c.sqldb == nil || c.cn == nil {
		return adbc.Error{Code: adbc.StatusInvalidState}
	}

	if err := c.sqldb.Close(); err != nil {
		return err
	}
	c.sqldb = nil

	defer func() {
		c.cn = nil
	}()
	return c.cn.Close()
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ReadPartition not yet implemented for snowflake driver",
	}
}

func (c *connectionImpl) SetOption(key, value string) error {
	switch key {
	case OptionUseHighPrecision:
		// statements will inherit the value of the OptionUseHighPrecision
		// from the connection, but the option can be overridden at the
		// statement level if SetOption is called on the statement.
		switch value {
		case adbc.OptionValueEnabled:
			c.useHighPrecision = true
		case adbc.OptionValueDisabled:
			c.useHighPrecision = false
		default:
			return adbc.Error{
				Msg:  "[Snowflake] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}
		return nil
	default:
		return adbc.Error{
			Msg:  "[Snowflake] unknown connection option " + key + ": " + value,
			Code: adbc.StatusInvalidArgument,
		}
	}
}
