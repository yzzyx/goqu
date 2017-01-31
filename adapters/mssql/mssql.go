package mssql

import (
	"errors"

	"gopkg.in/doug-martin/goqu.v3"
)

var (
	comma_rune       = ','
	space_rune       = ' '
	left_paren_rune  = '('
	right_paren_rune = ')'
	placeholder_rune = '?'
	quote_rune       = '"'
	singlq_quote     = '\''

	default_values_frag = []byte("")
	mssql_true          = []byte("1")
	mssql_false         = []byte("0")
	time_format         = "2006-01-02 15:04:05"
	operator_lookup     = map[goqu.BooleanOperation][]byte{
		goqu.EQ_OP:         []byte("="),
		goqu.NEQ_OP:        []byte("<>"),
		goqu.GT_OP:         []byte(">"),
		goqu.GTE_OP:        []byte(">="),
		goqu.LT_OP:         []byte("<"),
		goqu.LTE_OP:        []byte("<="),
		goqu.IN_OP:         []byte("IN"),
		goqu.NOT_IN_OP:     []byte("NOT IN"),
		goqu.IS_OP:         []byte("IS"),
		goqu.IS_NOT_OP:     []byte("IS NOT"),
		goqu.LIKE_OP:       []byte("LIKE"),
		goqu.NOT_LIKE_OP:   []byte("NOT LIKE"),
		goqu.I_LIKE_OP:     []byte("LIKE"),
		goqu.NOT_I_LIKE_OP: []byte("NOT LIKE"),

		// These are not actually supported in T-SQL
		goqu.REGEXP_LIKE_OP:       []byte("REGEXP"),
		goqu.REGEXP_NOT_LIKE_OP:   []byte("NOT REGEXP"),
		goqu.REGEXP_I_LIKE_OP:     []byte("REGEXP"),
		goqu.REGEXP_NOT_I_LIKE_OP: []byte("NOT REGEXP"),
	}
	escape_runes = map[rune][]byte{
		'\'': []byte("\\'"),
		'"':  []byte("\\\""),
		'\\': []byte("\\\\"),
		'\n': []byte("\\n"),
		'\r': []byte("\\r"),
		0:    []byte("\\x00"),
		0x1a: []byte("\\x1a"),
	}
)

type DatasetAdapter struct {
	*goqu.DefaultAdapter
	limit interface{}
	order goqu.ColumnList
}

func (me *DatasetAdapter) SupportsReturn() bool {
	return true
}

func (me *DatasetAdapter) SupportsLimitOnDelete() bool {
	return true
}

func (me *DatasetAdapter) SupportsLimitOnUpdate() bool {
	return true
}

func (me *DatasetAdapter) SupportsOrderByOnDelete() bool {
	return true
}

func (me *DatasetAdapter) SupportsOrderByOnUpdate() bool {
	return true
}

// OrderSql implements the same orderby-function as DefaultAdapter, but also
// saves a copy of the order-argument
func (me *DatasetAdapter) OrderSql(buf *goqu.SqlBuilder, order goqu.ColumnList) error {
	// We need to remember if we had a order-by clause, because limit and offset requires it
	me.order = order

	// This is standard
	if order != nil && len(order.Columns()) > 0 {
		buf.Write(me.OrderByFragment)
		return me.Literal(buf, order)
	}
	return nil
}

// LimitSql saves the limit-value for later use in offsetsql
func (me *DatasetAdapter) LimitSql(buf *goqu.SqlBuilder, limit interface{}) error {
	me.limit = limit
	return nil
}

/*
 MSSQL supports offset and limit as of SQLSERVER 2012,
 with the following form
 SELECT ... ORDER BY xxx OFFSET <offset> FETCH NEXT <limit> ROWS ONLY
 instead we need to wrap the query as a subquery with the following form:

 - OFFSET required ORDER BY to be set
 - FETCH NEXT cannot be used without OFFSET

 This means that we need to build the OFFSET and LIMIT in the "wrong" order
 compared to postgres, mysql and sqlite.
*/
func (me *DatasetAdapter) OffsetSql(buf *goqu.SqlBuilder, offset uint) error {
	var err error
	if me.limit != nil || offset > 0 {
		// Check if ORDER BY was added
		if me.order == nil {
			// We haven't added an ORDER BY clause, and it's needed for OFFSET
			buf.Write(me.OrderByFragment)
			err = me.Literal(buf, 1) // Column 1
			if err != nil {
				return err
			}
		}

		buf.Write([]byte(" OFFSET "))
		err = me.Literal(buf, offset)
		if err != nil {
			return err
		}
		buf.Write([]byte(" ROWS"))

		if me.limit != nil {
			buf.Write([]byte(" FETCH NEXT "))
			err = me.Literal(buf, me.limit)
			if err != nil {
				return err
			}
			buf.Write([]byte(" ROWS ONLY"))
		}
	}
	return nil
}

// ReturningSql always returns an error if columnlist is set for MSSQL, since it's not supported
func (me *DatasetAdapter) ReturningSql(buf *goqu.SqlBuilder, returns goqu.ColumnList) error {
	if returns != nil && len(returns.Columns()) > 0 {
		return errors.New("mssql-adapter does not support \"RETURNING\"")
	}
	return nil
}

// OnConflictSql always returns an error if conflictexpression is set for MSSQL, since it's not supported
func (me *DatasetAdapter) OnConflictSql(buf *goqu.SqlBuilder, o goqu.ConflictExpression) error {
	if o != nil {
		return errors.New("mssql-adapter does not support \"ON CONFLICT\"")
	}
	return nil
}

func newDatasetAdapter(ds *goqu.Dataset) goqu.Adapter {
	def := goqu.NewDefaultAdapter(ds).(*goqu.DefaultAdapter)
	def.PlaceHolderRune = placeholder_rune
	def.IncludePlaceholderNum = true
	def.QuoteRune = quote_rune
	def.DefaultValuesFragment = default_values_frag
	def.True = mssql_true
	def.False = mssql_false
	def.TimeFormat = time_format
	def.BooleanOperatorLookup = operator_lookup
	def.UseLiteralIsBools = false
	def.LimitFragment = []byte(" TOP ")
	def.EscapedRunes = escape_runes
	def.InsertIgnoreClause = []byte("")
	def.ConflictFragment = []byte("")
	def.ConflictDoUpdateFragment = []byte("")
	def.ConflictDoNothingFragment = []byte("")
	def.ConflictUpdateWhereSupported = false
	def.InsertIgnoreSyntaxSupported = false
	def.ConflictTargetSupported = false
	return &DatasetAdapter{def, nil, nil}
}

func init() {
	goqu.RegisterAdapter("mssql", newDatasetAdapter)
}
