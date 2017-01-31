package mssql

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/c2fo/testify/assert"
	"github.com/c2fo/testify/suite"
	_ "github.com/denisenkom/go-mssqldb"
	"gopkg.in/doug-martin/goqu.v3"
)

const schema = `
				IF OBJECT_ID(N'entry', N'U') IS NOT NULL DROP TABLE "entry";
				CREATE  TABLE "entry" (
						"id" INT IDENTITY(1, 1),
						"int" INT NOT NULL UNIQUE,
						"float" REAL NOT NULL,
						"string" VARCHAR(45) NOT NULL ,
						"time" DATETIME NOT NULL ,
						"bool" BIT NOT NULL ,
						"bytes" VARCHAR(45) NOT NULL);
				INSERT INTO "entry" ("int", "float", "string", "time", "bool", "bytes") VALUES
						(0, 0.000000, '0.000000', '2015-02-22 18:19:55', 1,  '0.000000'),
						(1, 0.100000, '0.100000', '2015-02-22 19:19:55', 0, '0.100000'),
						(2, 0.200000, '0.200000', '2015-02-22 20:19:55', 1,  '0.200000'),
						(3, 0.300000, '0.300000', '2015-02-22 21:19:55', 0, '0.300000'),
						(4, 0.400000, '0.400000', '2015-02-22 22:19:55', 1,  '0.400000'),
						(5, 0.500000, '0.500000', '2015-02-22 23:19:55', 0, '0.500000'),
						(6, 0.600000, '0.600000', '2015-02-23 00:19:55', 1,  '0.600000'),
						(7, 0.700000, '0.700000', '2015-02-23 01:19:55', 0, '0.700000'),
						(8, 0.800000, '0.800000', '2015-02-23 02:19:55', 1,  '0.800000'),
						(9, 0.900000, '0.900000', '2015-02-23 03:19:55', 0, '0.900000');
		`
const default_db_uri = "sqlserver://sqlserver:@127.0.0.1/"

var db_uri string

func init() {
	db_uri = os.Getenv("MSSQL_URI")
	if db_uri == "" {
		db_uri = default_db_uri
	}
}

type (
	logger    struct{}
	mssqlTest struct {
		suite.Suite
		db *goqu.Database
	}
	entry struct {
		Id     uint32    `db:"id" goqu:"skipinsert,skipupdate"`
		Int    int       `db:"int"`
		Float  float64   `db:"float"`
		String string    `db:"string"`
		Time   time.Time `db:"time"`
		Bool   bool      `db:"bool"`
		Bytes  []byte    `db:"bytes"`
	}
)

func (me logger) Printf(sql string, args ...interface{}) {
	fmt.Printf("\n"+sql, args)
}

func (me *mssqlTest) SetupSuite() {
	db, err := sql.Open("mssql", db_uri)
	if err != nil {
		panic(err.Error())
	}
	me.db = goqu.New("mssql", db)
}

func (me *mssqlTest) SetupTest() {
	if _, err := me.db.Exec(schema); err != nil {
		panic(err)
	}
}

func (me *mssqlTest) TestSelectSql() {
	t := me.T()
	ds := me.db.From("entry")
	sql, _, err := ds.Select("id", "float", "string", "time", "bool").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "float", "string", "time", "bool" FROM "entry"`)

	sql, _, err = ds.Where(goqu.I("int").Eq(10)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "entry" WHERE ("int" = 10)`)

	sql, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.I("int"), 10)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{10})
	assert.Equal(t, sql, `SELECT * FROM "entry" WHERE "int" = ?1`)

	sql, _, _ = ds.Where(goqu.ExOr{
		"col1": goqu.Op{"neq": 1},
		"col2": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).Order(goqu.I("col6").Desc()).Limit(10).ToSql()
	assert.Equal(t, sql, "SELECT * FROM \"entry\" WHERE ((\"col1\" <> 1) OR (\"col2\" NOT IN ('a', 'b', 'c'))) ORDER BY \"col6\" DESC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")
}

func (me *mssqlTest) TestQuery() {
	t := me.T()
	var entries []entry
	ds := me.db.From("entry")
	assert.NoError(t, ds.Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T18:19:55.000000000-00:00")
	assert.NoError(t, err)
	baseDate = baseDate.UTC()
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		assert.Equal(t, entry.Id, uint32(i+1))
		assert.Equal(t, entry.Int, i)
		assert.Equal(t, fmt.Sprintf("%f", entry.Float), f)
		assert.Equal(t, entry.String, f)
		assert.Equal(t, entry.Bytes, []byte(f))
		assert.Equal(t, entry.Bool, i%2 == 0)
		assert.Equal(t, entry.Time.Unix(), baseDate.Add(time.Duration(i)*time.Hour).Unix())
		floatVal += float64(0.1)
	}
	entries = entries[0:0]

	// FIXME - Handle IsTrue?
	//sql, _ ,_ := ds.Where(goqu.I("bool").IsTrue()).Order(goqu.I("id").Asc()).ToSql()
	// assert.NoError(t, ds.Where(goqu.I("bool").IsTrue()).Order(goqu.I("id").Asc()).ScanStructs(&entries))

	assert.NoError(t, ds.Where(goqu.I("bool").Eq(1)).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Bool)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("int").Gt(4)).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int > 4)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("int").Gte(5)).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int >= 5)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("int").Lt(5)).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int < 5)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("int").Lte(4)).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int <= 4)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("int").Between(goqu.RangeVal{Start: 3, End: 6})).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 4)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int >= 3)
		assert.True(t, entry.Int <= 6)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("string").Eq("0.100000")).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 1)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("string").Like("0.1%")).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 1)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("string").NotLike("0.1%")).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 9)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.I("string").IsNull()).Order(goqu.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 0)
}

func (me *mssqlTest) TestCount() {
	t := me.T()
	ds := me.db.From("entry")
	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 10)
	count, err = ds.Where(goqu.I("int").Gt(4)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 5)
	count, err = ds.Where(goqu.I("int").Gte(4)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 6)
	count, err = ds.Where(goqu.I("string").Like("0.1%")).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 1)
	count, err = ds.Where(goqu.I("string").IsNull()).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 0)
}

func (me *mssqlTest) TestInsert() {
	t := me.T()
	ds := me.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert(e).Exec()
	assert.NoError(t, err)

	var insertedEntry entry
	found, err := ds.Where(goqu.I("int").Eq(10)).ScanStruct(&insertedEntry)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, insertedEntry.Id > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert(entries).Exec()
	assert.NoError(t, err)

	var newEntries []entry
	assert.NoError(t, ds.Where(goqu.I("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)

	_, err = ds.Insert(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Exec()
	assert.NoError(t, err)

	newEntries = newEntries[0:0]
	assert.NoError(t, ds.Where(goqu.I("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)
}

func (me *mssqlTest) TestInsertReturning() {
	t := me.T()
	ds := me.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Returning(goqu.Star()).Insert(e).ScanStruct(&e)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "mssql-adapter does not support \"RETURNING\"")
}

func (me *mssqlTest) TestUpdate() {
	t := me.T()
	ds := me.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.I("int").Eq(9)).Select(goqu.Star()).ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	e.Int = 11
	_, err = ds.Where(goqu.I("id").Eq(e.Id)).Update(e).Exec()
	assert.NoError(t, err)

	count, err := ds.Where(goqu.I("int").Eq(11)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 1)
}

func (me *mssqlTest) TestDelete() {
	t := me.T()
	ds := me.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.I("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	_, err = ds.Where(goqu.I("id").Eq(e.Id)).Delete().Exec()
	assert.NoError(t, err)

	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 9)

	var id uint32
	found, err = ds.Where(goqu.I("id").Eq(e.Id)).ScanVal(&id)
	assert.NoError(t, err)
	assert.False(t, found)

	e = entry{}
	found, err = ds.Where(goqu.I("int").Eq(8)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotEqual(t, e.Id, 0)

	id = 0
	_, err = ds.Where(goqu.I("id").Eq(e.Id)).Returning("id").Delete().ScanVal(&id)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "goqu: mssql-adapter does not support \"RETURNING\"")
}

func (me *mssqlTest) TestInsertIgnore() {
	t := me.T()
	ds := me.db.From("entry")
	now := time.Now()

	//insert one
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
		{Int: 10, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err := ds.InsertIgnore(entries).Exec()
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "mssql-adapter does not support \"ON CONFLICT\"")
}

func (me *mssqlTest) TestInsertConflict() {
	t := me.T()
	ds := me.db.From("entry")
	now := time.Now()

	//DO NOTHING insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.InsertConflict(goqu.DoNothing(), e).Exec()
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "mssql-adapter does not support \"ON CONFLICT\"")
}

func TestMssqlSuite(t *testing.T) {
	suite.Run(t, new(mssqlTest))
}
