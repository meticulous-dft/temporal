package mongodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

func TestQueryConverterBuildsNativeMongoDBPredicates(t *testing.T) {
	t.Parallel()
	converter := &queryConverter{}
	keyword := query.NewSAColumn("CustomKeyword", "Keyword01", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	integer := query.NewSAColumn("CustomInt", "Int01", enumspb.INDEXED_VALUE_TYPE_INT)

	equal, err := converter.ConvertKeywordComparisonExpr(sqlparser.EqualStr, keyword, "value")
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "Keyword01", Value: "value"}}, equal)

	in, err := converter.ConvertComparisonExpr(sqlparser.InStr, integer, []any{int64(2), int64(3)})
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "Int01", Value: bson.D{{Key: "$in", Value: []any{int64(2), int64(3)}}}}}, in)

	prefix, err := converter.ConvertKeywordComparisonExpr(sqlparser.StartsWithStr, keyword, "literal.+")
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "Keyword01", Value: primitive.Regex{Pattern: `^literal\.\+`}}}, prefix)

	notPrefix, err := converter.ConvertKeywordComparisonExpr(sqlparser.NotStartsWithStr, keyword, "blocked")
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "$nor", Value: bson.A{
		bson.D{{Key: "Keyword01", Value: primitive.Regex{Pattern: "^blocked"}}},
	}}}, notPrefix)
}

func TestQueryConverterBuildsTextRangeNullAndLogicalPredicates(t *testing.T) {
	t.Parallel()
	converter := &queryConverter{}
	text := query.NewSAColumn("CustomText", "Text01", enumspb.INDEXED_VALUE_TYPE_TEXT)
	datetime := query.NewSAColumn("CustomDatetime", "Datetime01", enumspb.INDEXED_VALUE_TYPE_DATETIME)
	from := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	to := from.Add(time.Hour)

	textExpr, err := converter.ConvertTextComparisonExpr(sqlparser.EqualStr, text, "one  two")
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "_text_tokens.Text01", Value: bson.D{{Key: "$in", Value: bson.A{"one", "two"}}}}}, textExpr)

	rangeExpr, err := converter.ConvertRangeExpr(sqlparser.BetweenStr, datetime, from, to)
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "Datetime01", Value: bson.D{
		{Key: "$gte", Value: from.UnixNano()},
		{Key: "$lte", Value: to.UnixNano()},
	}}}, rangeExpr)

	nullExpr, err := converter.ConvertIsExpr(sqlparser.IsNullStr, datetime)
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "Datetime01", Value: bson.D{{Key: "$exists", Value: false}}}}, nullExpr)

	combined, err := converter.BuildAndExpr(nil, textExpr, rangeExpr)
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "$and", Value: bson.A{textExpr, rangeExpr}}}, combined)
}

func TestQueryConverterRejectsEmptyTextAndUnsupportedRange(t *testing.T) {
	t.Parallel()
	converter := &queryConverter{}
	text := query.NewSAColumn("CustomText", "Text01", enumspb.INDEXED_VALUE_TYPE_TEXT)

	_, err := converter.ConvertTextComparisonExpr(sqlparser.EqualStr, text, "   ")
	require.ErrorContains(t, err, "no tokens found")

	_, err = converter.ConvertRangeExpr("unexpected", text, "a", "b")
	require.Error(t, err)
}
