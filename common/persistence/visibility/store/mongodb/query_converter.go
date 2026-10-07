package mongodb

import (
	"regexp"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

const textTokensField = "_text_tokens"

type queryConverter struct{}

var _ query.StoreQueryConverter[bson.D] = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

func (c *queryConverter) BuildParenExpr(expr bson.D) (bson.D, error) {
	return expr, nil
}

func (c *queryConverter) BuildNotExpr(expr bson.D) (bson.D, error) {
	if len(expr) == 0 {
		return nil, nil
	}
	return bson.D{{Key: "$nor", Value: bson.A{expr}}}, nil
}

func (c *queryConverter) BuildAndExpr(exprs ...bson.D) (bson.D, error) {
	return buildLogicalExpression("$and", exprs), nil
}

func (c *queryConverter) BuildOrExpr(exprs ...bson.D) (bson.D, error) {
	return buildLogicalExpression("$or", exprs), nil
}

func buildLogicalExpression(operator string, exprs []bson.D) bson.D {
	valid := make([]bson.D, 0, len(exprs))
	for _, expr := range exprs {
		if len(expr) != 0 {
			valid = append(valid, expr)
		}
	}
	if len(valid) == 0 {
		return nil
	}
	if len(valid) == 1 {
		return valid[0]
	}
	values := make(bson.A, len(valid))
	for i, expr := range valid {
		values[i] = expr
	}
	return bson.D{{Key: operator, Value: values}}
}

func (c *queryConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.D, error) {
	var err error
	value, err = mongoQueryValue(col, value)
	if err != nil {
		return nil, err
	}
	return comparisonExpression(operator, col, col.FieldName, value)
}

func (c *queryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.D, error) {
	if operator != sqlparser.StartsWithStr && operator != sqlparser.NotStartsWithStr {
		return c.ConvertComparisonExpr(operator, col, value)
	}
	prefix, ok := value.(string)
	if !ok {
		return nil, query.NewConverterError(
			"%s: right-hand side of operator '%s' must be a string",
			query.InvalidExpressionErrMessage,
			strings.ToUpper(operator),
		)
	}
	expr := bson.D{{Key: col.FieldName, Value: primitive.Regex{Pattern: "^" + regexp.QuoteMeta(prefix)}}}
	if operator == sqlparser.NotStartsWithStr {
		return c.BuildNotExpr(expr)
	}
	return expr, nil
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.D, error) {
	return c.ConvertComparisonExpr(operator, col, value)
}

func (c *queryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.D, error) {
	text, ok := value.(string)
	if !ok {
		return nil, query.NewConverterError(
			"%s: right-hand side of operator '%s' must be a string",
			query.InvalidExpressionErrMessage,
			strings.ToUpper(operator),
		)
	}
	tokens := query.TokenizeTextQueryString(text)
	if len(tokens) == 0 {
		return nil, query.NewConverterError(
			"%s: unexpected value for Text type search attribute (no tokens found)",
			query.InvalidExpressionErrMessage,
		)
	}
	values := make(bson.A, len(tokens))
	for index, token := range tokens {
		values[index] = token
	}
	expr := bson.D{{Key: textTokensField + "." + col.FieldName, Value: bson.D{{Key: "$in", Value: values}}}}
	if operator == sqlparser.NotEqualStr {
		return c.BuildNotExpr(expr)
	}
	return expr, nil
}

func (c *queryConverter) ConvertRangeExpr(
	operator string,
	col *query.SAColumn,
	from any,
	to any,
) (bson.D, error) {
	var err error
	from, err = mongoQueryValue(col, from)
	if err != nil {
		return nil, err
	}
	to, err = mongoQueryValue(col, to)
	if err != nil {
		return nil, err
	}
	expr := bson.D{{Key: col.FieldName, Value: bson.D{
		{Key: "$gte", Value: from},
		{Key: "$lte", Value: to},
	}}}
	switch operator {
	case sqlparser.BetweenStr:
		return expr, nil
	case sqlparser.NotBetweenStr:
		return c.BuildNotExpr(expr)
	default:
		return nil, query.NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}
}

func mongoQueryValue(col *query.SAColumn, value any) (any, error) {
	if col.ValueType != enumspb.INDEXED_VALUE_TYPE_DATETIME {
		return value, nil
	}
	if values, ok := value.([]any); ok {
		converted := make([]any, len(values))
		for index, item := range values {
			var err error
			converted[index], err = mongoQueryValue(col, item)
			if err != nil {
				return nil, err
			}
		}
		return converted, nil
	}
	if datetime, ok := value.(time.Time); ok {
		return datetime.UnixNano(), nil
	}
	formatted, ok := value.(string)
	if !ok {
		return nil, query.NewConverterError(
			"%s: datetime search attribute %q has unexpected value %T",
			query.InvalidExpressionErrMessage,
			col.Alias,
			value,
		)
	}
	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	if err != nil {
		return nil, query.NewConverterError(
			"%s: unable to parse datetime %q",
			query.InvalidExpressionErrMessage,
			formatted,
		)
	}
	return parsed.UnixNano(), nil
}

func (c *queryConverter) ConvertIsExpr(operator string, col *query.SAColumn) (bson.D, error) {
	switch operator {
	case sqlparser.IsNullStr:
		return bson.D{{Key: col.FieldName, Value: bson.D{{Key: "$exists", Value: false}}}}, nil
	case sqlparser.IsNotNullStr:
		return bson.D{{Key: col.FieldName, Value: bson.D{{Key: "$exists", Value: true}}}}, nil
	default:
		return nil, query.NewConverterError(
			"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
			query.InvalidExpressionErrMessage,
		)
	}
}

func comparisonExpression(operator string, col *query.SAColumn, field string, value any) (bson.D, error) {
	var mongoOperator string
	switch operator {
	case sqlparser.EqualStr:
		return bson.D{{Key: field, Value: value}}, nil
	case sqlparser.NotEqualStr:
		mongoOperator = "$ne"
	case sqlparser.LessThanStr:
		mongoOperator = "$lt"
	case sqlparser.GreaterThanStr:
		mongoOperator = "$gt"
	case sqlparser.LessEqualStr:
		mongoOperator = "$lte"
	case sqlparser.GreaterEqualStr:
		mongoOperator = "$gte"
	case sqlparser.InStr:
		mongoOperator = "$in"
	case sqlparser.NotInStr:
		mongoOperator = "$nin"
	default:
		return nil, query.NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}
	return bson.D{{Key: field, Value: bson.D{{Key: mongoOperator, Value: value}}}}, nil
}
