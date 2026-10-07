package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.temporal.io/api/serviceerror"
)

func TestMongoUnavailableErrorPreservesCauseAndServiceType(t *testing.T) {
	cause := mongo.CommandError{
		Code:    112,
		Message: "write conflict",
		Labels:  []string{"TransientTransactionError"},
	}
	err := newMongoUnavailableErrorf(cause, "write failed: %v", cause)

	var labeled mongo.LabeledError
	require.ErrorAs(t, err, &labeled)
	require.True(t, labeled.HasErrorLabel("TransientTransactionError"))

	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "write failed: write conflict", unavailable.Message)
	require.Equal(t, unavailable.Status(), serviceerror.ToStatus(err))
}
