//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

// These bounded-record prototypes do not replace the adapter's storage protocol.
func TestMongoDBModelTransactionLocality(t *testing.T) {
	fixture, cfg := newFencingTestFactory(t, "model_locality")
	requireShardedTopology(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	factory := newModelMeasurementFactory(ctx, t, fixture, cfg)
	t.Run("unrelated_transaction_not_measured", func(t *testing.T) {
		const name = "counter_noise"
		createModelCollection(ctx, t, fixture, cfg.DatabaseName, name, "shard-rs-0")
		collection := factory.database.Collection(name)
		_, err := runMeasuredModelTransaction(ctx, t, factory, 1, func(txCtx context.Context) (interface{}, error) {
			if _, err := collection.InsertOne(txCtx, bson.M{"group": 10, "record": "owned"}); err != nil {
				return nil, err
			}
			return runModelTransaction(ctx, factory, func(otherCtx context.Context) (interface{}, error) {
				return collection.InsertOne(otherCtx, bson.M{"group": 70, "record": "unrelated"})
			})
		})
		require.NoError(t, err)
		count, err := collection.CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Equal(t, int64(2), count, "both transactions commit but only the owned session is measured")
	})
	for _, shape := range []struct {
		name    string
		records []string
	}{
		{"history", []string{"fence", "current", "old_run", "new_run", "outbox"}},
		{"matching", []string{"fence", "backlog"}},
	} {
		for _, layout := range []string{"separate_distributed", "separate_colocated", "bounded_records"} {
			t.Run(shape.name+"/"+layout, func(t *testing.T) {
				collections := make([]client.Collection, len(shape.records))
				for i, record := range shape.records {
					name := shape.name + "_" + layout
					if layout != "bounded_records" {
						name += "_" + record
					}
					if i == 0 || layout != "bounded_records" {
						target := "shard-rs-0"
						if layout == "separate_distributed" && i%2 == 1 {
							target = "shard-rs-1"
						}
						createModelCollection(ctx, t, fixture, cfg.DatabaseName, name, target)
					}
					collections[i] = factory.database.Collection(name)
					_, err := collections[i].InsertOne(ctx, bson.M{"group": 10, "record": record, "revision": 0, "payload": make([]byte, 1024)})
					require.NoError(t, err)
				}
				before := readModelTransactionCounters(ctx, t, factory)
				shards := 1
				if layout == "separate_distributed" {
					shards = 2
				}
				const commits = 20
				latencies := make([]time.Duration, 0, commits)
				attempts := 0
				for revision := 1; revision <= commits; revision++ {
					start := time.Now()
					_, err := runMeasuredModelTransaction(ctx, t, factory, shards, func(txCtx context.Context) (interface{}, error) {
						attempts++
						for i, record := range shape.records {
							result, err := collections[i].UpdateOne(txCtx, bson.M{"group": 10, "record": record, "revision": revision - 1}, bson.M{"$set": bson.M{"revision": revision}})
							if err != nil {
								return nil, err
							}
							if result.MatchedCount != 1 {
								return nil, fmt.Errorf("record %s lost expected revision %d (matched %d)", record, revision-1, result.MatchedCount)
							}
						}
						return nil, nil
					})
					require.NoError(t, err)
					latencies = append(latencies, time.Since(start))
				}
				after := readModelTransactionCounters(ctx, t, factory)
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				t.Logf("commits=%d callback_attempts=%d counters_before=%+v counters_after=%+v p50=%s p95=%s (instrumented local sequential probe, not a capacity benchmark)", commits, attempts, before, after, latencies[9], latencies[18])
				require.GreaterOrEqual(t, attempts, commits)
				abort := errors.New("stop before committing bounded records")
				_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
					for i, record := range shape.records {
						if _, err := collections[i].UpdateOne(txCtx, bson.M{"group": 10, "record": record}, bson.M{"$set": bson.M{"revision": commits + 1}}); err != nil {
							return nil, err
						}
					}
					return nil, abort
				})
				require.ErrorIs(t, err, abort)
				for i, record := range shape.records {
					var doc struct{ Revision int }
					require.NoError(t, collections[i].FindOne(ctx, bson.M{"group": 10, "record": record}).Decode(&doc))
					require.Equal(t, commits, doc.Revision, "durable content/rollback for %s", record)
				}
				if layout == "separate_distributed" {
					return
				}
				movedName := shape.name + "_" + layout
				if layout == "separate_colocated" {
					movedName += "_" + shape.records[0]
				}
				require.NoError(t, runAdminCommand(ctx, fixture, bson.D{
					{Key: "moveChunk", Value: cfg.DatabaseName + "." + movedName},
					{Key: "find", Value: bson.D{{Key: "group", Value: 10}}}, {Key: "to", Value: "shard-rs-1"},
				}))
				require.Equal(t, "shard-rs-1", explainTargetShard(ctx, t, fixture, movedName, bson.D{{Key: "group", Value: 10}}))
				before = readModelTransactionCounters(ctx, t, factory)
				shards = 1
				if layout == "separate_colocated" {
					shards = 2
				}
				_, err = runMeasuredModelTransaction(ctx, t, factory, shards, func(txCtx context.Context) (interface{}, error) {
					for i, record := range shape.records {
						if _, err := collections[i].UpdateOne(txCtx, bson.M{"group": 10, "record": record}, bson.M{"$inc": bson.M{"revision": 1}}); err != nil {
							return nil, err
						}
					}
					return nil, nil
				})
				require.NoError(t, err)
				after = readModelTransactionCounters(ctx, t, factory)
				t.Logf("after migration: shards=%d counters_before=%+v counters_after=%+v", shards, before, after)
				for i, record := range shape.records {
					var doc struct{ Revision int }
					require.NoError(t, collections[i].FindOne(ctx, bson.M{"group": 10, "record": record}).Decode(&doc))
					require.Equal(t, commits+1, doc.Revision)
				}
			})
		}
	}
}

func newModelMeasurementFactory(ctx context.Context, t *testing.T, fixture *Factory, cfg config.MongoDB) *Factory {
	t.Helper()
	measurementRouter := os.Getenv("TEMPORAL_MONGODB_MODEL_ROUTER")
	require.NotEmpty(t, measurementRouter, "owned transaction measurements require a dedicated router")
	require.NotContains(t, cfg.Hosts, measurementRouter, "fixture DDL must use a different router")
	cfg.Hosts = []string{measurementRouter}
	factory, err := NewFactory(cfg, "model-measurement", fixture.logger, fixture.metricsHandler)
	require.NoError(t, err)
	t.Cleanup(factory.Close)
	var logging struct{ LogComponentVerbosity bson.M }
	require.NoError(t, factory.client.Database("admin").RunCommand(ctx, bson.D{{Key: "getParameter", Value: 1}, {Key: "logComponentVerbosity", Value: 1}}).Decode(&logging))
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{{Key: "setParameter", Value: 1}, {Key: "logComponentVerbosity", Value: bson.M{"transaction": bson.M{"verbosity": 1}}}}))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, runAdminCommand(cleanupCtx, factory, bson.D{{Key: "setParameter", Value: 1}, {Key: "logComponentVerbosity", Value: logging.LogComponentVerbosity}}))
	})
	return factory
}

func createModelCollection(ctx context.Context, t *testing.T, factory *Factory, database, name, target string) {
	t.Helper()
	_, err := factory.database.Collection(name).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "group", Value: 1}, {Key: "record", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "shardCollection", Value: database + "." + name},
		{Key: "key", Value: bson.D{{Key: "group", Value: 1}}},
	}))
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "split", Value: database + "." + name}, {Key: "middle", Value: bson.D{{Key: "group", Value: 50}}},
	}))
	initial := "shard-rs-1"
	if target == initial {
		initial = "shard-rs-0"
	}
	// Complete initial cleanup before testing a return migration to the same range.
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "moveChunk", Value: database + "." + name},
		{Key: "find", Value: bson.D{{Key: "group", Value: 10}}}, {Key: "to", Value: initial},
		{Key: "_waitForDelete", Value: true},
	}))
	for _, placement := range []struct {
		group int
		shard string
	}{{10, target}, {70, "shard-rs-1"}} {
		require.NoError(t, runAdminCommand(ctx, factory, bson.D{
			{Key: "moveChunk", Value: database + "." + name},
			{Key: "find", Value: bson.D{{Key: "group", Value: placement.group}}}, {Key: "to", Value: placement.shard},
			{Key: "_waitForDelete", Value: true},
		}))
		require.Equal(t, placement.shard, explainTargetShard(ctx, t, factory, name, bson.D{{Key: "group", Value: placement.group}}))
	}
}

func runModelTransaction(ctx context.Context, factory *Factory, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	session, err := factory.client.StartSession(ctx)
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)
	return session.WithTransaction(ctx, fn)
}

type modelTransactionCounters struct {
	Participants     int64
	Started          int64
	Aborted          int64
	Committed        int64
	ReadOnly         modelCommitCounters
	SingleWriteShard modelCommitCounters
	SingleShard      modelCommitCounters
	TwoPhase         modelCommitCounters
}

type modelCommitCounters struct{ Initiated, Successful int64 }

func runMeasuredModelTransaction(ctx context.Context, t *testing.T, factory *Factory, shards int, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	t.Helper()
	before := readModelTransactionLog(ctx, t, factory)
	var sessionID []byte
	value, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
		_, id, ok := mongo.SessionFromContext(txCtx).ID().Lookup("id").BinaryOK()
		if !ok {
			return nil, errors.New("measured transaction has no session ID")
		}
		sessionID = append([]byte(nil), id...)
		return fn(txCtx)
	})
	if err != nil {
		return value, err
	}
	after := readModelTransactionLog(ctx, t, factory)
	newLines := after.TotalLinesWritten - before.TotalLinesWritten
	require.Positive(t, newLines, "transaction logging must capture the measured commit")
	require.LessOrEqual(t, newLines, int64(len(after.Log)), "transaction evidence overflowed the log buffer")
	commitType := "singleShard"
	if shards == 2 {
		commitType = "twoPhaseCommit"
	}
	committed := 0
	for _, line := range after.Log[int64(len(after.Log))-newLines:] {
		if !strings.Contains(line, `"msg":"transaction"`) {
			continue
		}
		var entry struct {
			Attr struct {
				Parameters struct {
					LSID struct{ ID primitive.Binary }
				}
				NumParticipants  int
				TerminationCause string
				CommitType       string
			}
		}
		require.NoError(t, bson.UnmarshalExtJSON([]byte(line), false, &entry))
		if !bytes.Equal(entry.Attr.Parameters.LSID.ID.Data, sessionID) || entry.Attr.TerminationCause != "committed" {
			continue
		}
		committed++
		require.Equal(t, shards, entry.Attr.NumParticipants, "owned transaction: %s", line)
		require.Equal(t, commitType, entry.Attr.CommitType, "owned transaction: %s", line)
	}
	require.Equal(t, 1, committed, "exactly one successful owned transaction must be captured")
	return value, nil
}

type modelTransactionLog struct {
	TotalLinesWritten int64
	Log               []string
}

func readModelTransactionLog(ctx context.Context, t *testing.T, factory *Factory) modelTransactionLog {
	t.Helper()
	var result modelTransactionLog
	require.NoError(t, factory.client.Database("admin").RunCommand(ctx, bson.D{{Key: "getLog", Value: "global"}}).Decode(&result))
	return result
}

func readModelTransactionCounters(ctx context.Context, t *testing.T, factory *Factory) modelTransactionCounters {
	t.Helper()
	var status struct {
		Transactions struct {
			Participants int64 `bson:"totalParticipantsAtCommit"`
			Started      int64 `bson:"totalStarted"`
			Aborted      int64 `bson:"totalAborted"`
			Committed    int64 `bson:"totalCommitted"`
			CommitTypes  struct {
				ReadOnly         modelCommitCounters `bson:"readOnly"`
				SingleWriteShard modelCommitCounters `bson:"singleWriteShard"`
				SingleShard      modelCommitCounters `bson:"singleShard"`
				TwoPhase         modelCommitCounters `bson:"twoPhaseCommit"`
			} `bson:"commitTypes"`
		}
	}
	require.NoError(t, factory.client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status))
	return modelTransactionCounters{Participants: status.Transactions.Participants, Started: status.Transactions.Started, Aborted: status.Transactions.Aborted, Committed: status.Transactions.Committed, ReadOnly: status.Transactions.CommitTypes.ReadOnly, SingleWriteShard: status.Transactions.CommitTypes.SingleWriteShard, SingleShard: status.Transactions.CommitTypes.SingleShard, TwoPhase: status.Transactions.CommitTypes.TwoPhase}
}
