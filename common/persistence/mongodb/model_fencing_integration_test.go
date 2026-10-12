//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

func TestMongoDBModelBucketFencing(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "model_fences")
	requireShardedTopology(t, factory)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	const name = "bucket_records"
	createModelCollection(ctx, t, factory, cfg.DatabaseName, name, "shard-rs-0")
	collection := factory.database.Collection(name)
	for _, key := range []bson.D{
		{{Key: "logical_shard", Value: 1}, {Key: "kind", Value: 1}, {Key: "task_id", Value: 1}},
		{{Key: "logical_shard", Value: 1}, {Key: "kind", Value: 1}, {Key: "fire_time", Value: 1}, {Key: "task_id", Value: 1}},
	} {
		_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: key})
		require.NoError(t, err)
	}
	for _, group := range []int{10, 70} {
		_, err := collection.InsertOne(ctx, bson.M{"group": group, "record": "fence", "epoch": 100, "writes": 0})
		require.NoError(t, err)
	}
	_, err := collection.InsertOne(ctx, bson.M{"group": 10, "record": "owner", "epoch": 100})
	require.NoError(t, err)
	lost := errors.New("storage group ownership lost")
	fence := func(txCtx context.Context, group, epoch int) error {
		result, err := collection.UpdateOne(txCtx, bson.M{"group": group, "record": "fence", "epoch": epoch}, bson.M{"$inc": bson.M{"writes": 1}})
		if err != nil {
			return err
		}
		if result.MatchedCount != 1 {
			return lost
		}
		return nil
	}
	write := func(group, epoch int, taskID int64) error {
		_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
			if err := fence(txCtx, group, epoch); err != nil {
				return nil, err
			}
			_, err := collection.InsertMany(txCtx, []interface{}{
				bson.M{"group": group, "record": fmt.Sprintf("state_%d", taskID), "logical_shard": 17, "epoch": epoch},
				bson.M{"group": group, "record": fmt.Sprintf("task_%d", taskID), "logical_shard": 17, "kind": "task", "task_id": taskID},
			})
			return nil, err
		})
		return err
	}
	takeover := func(oldEpoch, newEpoch int, afterFirst func(context.Context) error) error {
		_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
			for i, group := range []int{10, 70} {
				result, err := collection.UpdateOne(txCtx, bson.M{"group": group, "record": "fence", "epoch": oldEpoch}, bson.M{"$set": bson.M{"epoch": newEpoch}})
				if err != nil {
					return nil, err
				}
				if result.MatchedCount != 1 {
					return nil, lost
				}
				if i == 0 && afterFirst != nil {
					if err := afterFirst(txCtx); err != nil {
						return nil, err
					}
				}
			}
			result, err := collection.UpdateOne(txCtx, bson.M{"group": 10, "record": "owner", "epoch": oldEpoch}, bson.M{"$set": bson.M{"epoch": newEpoch}})
			if err != nil {
				return nil, err
			}
			if result.MatchedCount != 1 {
				return nil, lost
			}
			return nil, nil
		})
		return err
	}
	t.Run("independent_bucket_progress", func(t *testing.T) {
		pause := newFencingPause()
		t.Cleanup(pause.release)
		abort := errors.New("abort held transaction")
		done := make(chan error, 1)
		go func() {
			_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
				if err := fence(txCtx, 10, 100); err != nil {
					return nil, err
				}
				if err := pause.wait(txCtx); err != nil {
					return nil, err
				}
				return nil, abort
			})
			done <- err
		}()
		waitModelPause(ctx, t, pause)
		require.NoError(t, write(70, 100, 1), "other physical shard must progress while bucket 10 is write-locked")
		pause.release()
		require.ErrorIs(t, waitModelResult(ctx, t, done), abort)
	})
	t.Run("failed_takeover_is_atomic", func(t *testing.T) {
		abort := errors.New("fail after fencing first group")
		require.ErrorIs(t, takeover(100, 101, func(context.Context) error { return abort }), abort)
		requireModelEpochs(ctx, t, collection, 100)
		require.ErrorIs(t, write(70, 101, 99), lost, "new owner must not write before publication")
		require.NoError(t, write(10, 100, 2))
	})
	t.Run("old_writer_forces_takeover_retry", func(t *testing.T) {
		pause := newFencingPause()
		t.Cleanup(pause.release)
		var attempts atomic.Int32
		done := make(chan error, 1)
		go func() {
			done <- takeover(100, 101, func(txCtx context.Context) error {
				if attempts.Add(1) == 1 {
					return pause.wait(txCtx)
				}
				return nil
			})
		}()
		waitModelPause(ctx, t, pause)
		var owner struct{ Epoch int }
		require.NoError(t, collection.FindOne(ctx, bson.M{"group": 10, "record": "owner"}).Decode(&owner))
		require.Equal(t, 100, owner.Epoch, "partially fenced ownership must remain unpublished")
		require.ErrorIs(t, write(70, 101, 99), lost)
		require.NoError(t, write(70, 100, 3))
		pause.release()
		require.NoError(t, waitModelResult(ctx, t, done))
		require.GreaterOrEqual(t, attempts.Load(), int32(2), "takeover must retry its stale snapshot, not publish partially fenced ownership")
		requireModelEpochs(ctx, t, collection, 101)
	})
	t.Run("stale_writer_after_publication", func(t *testing.T) {
		pause := newFencingPause()
		t.Cleanup(pause.release)
		done := make(chan error, 1)
		go func() {
			_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
				var oldSnapshot struct{ Epoch int }
				if err := collection.FindOne(txCtx, bson.M{"group": 70, "record": "fence"}).Decode(&oldSnapshot); err != nil {
					return nil, err
				}
				if err := pause.wait(txCtx); err != nil {
					return nil, err
				}
				return nil, fence(txCtx, 70, 101)
			})
			done <- err
		}()
		waitModelPause(ctx, t, pause)
		require.NoError(t, takeover(101, 102, nil))
		pause.release()
		require.ErrorIs(t, waitModelResult(ctx, t, done), lost)
		require.ErrorIs(t, write(10, 101, 99), lost)
		require.NoError(t, write(10, 102, 4))
		requireModelEpochs(ctx, t, collection, 102)
	})
	t.Run("ordered_task_fanout_and_ack", func(t *testing.T) {
		filter := bson.D{{Key: "logical_shard", Value: 17}, {Key: "kind", Value: "task"}, {Key: "task_id", Value: bson.M{"$gte": 1, "$lt": 5}}}
		require.Equal(t, 2, explainShardCount(ctx, t, factory, name, filter))
		cursor, err := collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "task_id", Value: 1}}).SetLimit(2))
		require.NoError(t, err)
		var page []struct {
			TaskID int64 `bson:"task_id"`
		}
		require.NoError(t, cursor.All(ctx, &page))
		require.Len(t, page, 2)
		require.Equal(t, []int64{1, 2}, []int64{page[0].TaskID, page[1].TaskID})
		result, err := collection.DeleteMany(ctx, bson.M{"logical_shard": 17, "kind": "task", "task_id": bson.M{"$lt": 3}})
		require.NoError(t, err)
		require.Equal(t, int64(2), result.DeletedCount)
		count, err := collection.CountDocuments(ctx, bson.M{"logical_shard": 17, "kind": "task"})
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
		count, err = collection.CountDocuments(ctx, bson.M{"logical_shard": 17, "record": bson.M{"$regex": "^state_"}})
		require.NoError(t, err)
		require.Equal(t, int64(4), count, "task acknowledgement must not delete mutable state")
	})
	t.Run("scheduled_task_order_and_boundary_ack", func(t *testing.T) {
		for _, task := range []struct {
			group int
			id    int64
			fire  int64
		}{{70, 5, 1000}, {10, 6, 1000}, {70, 7, 2000}} {
			_, err := collection.InsertOne(ctx, bson.M{"group": task.group, "record": fmt.Sprintf("timer_%d", task.id), "logical_shard": 17, "kind": "timer", "fire_time": task.fire, "task_id": task.id})
			require.NoError(t, err)
		}
		filter := bson.D{{Key: "logical_shard", Value: 17}, {Key: "kind", Value: "timer"}, {Key: "fire_time", Value: bson.M{"$gte": 1000, "$lt": 2000}}}
		require.Equal(t, 2, explainShardCount(ctx, t, factory, name, filter))
		cursor, err := collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "fire_time", Value: 1}, {Key: "task_id", Value: 1}}))
		require.NoError(t, err)
		var page []struct {
			TaskID int64 `bson:"task_id"`
		}
		require.NoError(t, cursor.All(ctx, &page))
		require.Len(t, page, 2)
		require.Equal(t, []int64{5, 6}, []int64{page[0].TaskID, page[1].TaskID})
		result, err := collection.DeleteMany(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(2), result.DeletedCount)
		var remaining struct {
			TaskID int64 `bson:"task_id"`
		}
		require.NoError(t, collection.FindOne(ctx, bson.M{"group": 70, "record": "timer_7"}).Decode(&remaining))
		require.Equal(t, int64(7), remaining.TaskID, "exclusive scheduled boundary must survive acknowledgement")
	})
	t.Run("snapshot_only_lease_is_not_a_fence", func(t *testing.T) {
		pause := newFencingPause()
		t.Cleanup(pause.release)
		done := make(chan error, 1)
		go func() {
			_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
				var lease struct{ Epoch int }
				if err := collection.FindOne(txCtx, bson.M{"group": 70, "record": "fence", "epoch": 102}).Decode(&lease); err != nil {
					return nil, err
				}
				if err := pause.wait(txCtx); err != nil {
					return nil, err
				}
				_, err := collection.InsertOne(txCtx, bson.M{"group": 70, "record": "unsafe_stale_state", "epoch": lease.Epoch})
				return nil, err
			})
			done <- err
		}()
		waitModelPause(ctx, t, pause)
		require.NoError(t, takeover(102, 103, nil))
		pause.release()
		require.NoError(t, waitModelResult(ctx, t, done), "negative control: a lease read alone permits stale commit")
		var stale struct{ Epoch int }
		require.NoError(t, collection.FindOne(ctx, bson.M{"group": 70, "record": "unsafe_stale_state"}).Decode(&stale))
		require.Equal(t, 102, stale.Epoch)
		requireModelEpochs(ctx, t, collection, 103)
	})
}

func waitModelPause(ctx context.Context, t *testing.T, pause *fencingPause) {
	t.Helper()
	select {
	case <-pause.reached:
	case <-ctx.Done():
		t.Fatal("timed out waiting for transaction barrier")
	}
}

func waitModelResult(ctx context.Context, t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatal("timed out waiting for transaction outcome")
		return ctx.Err()
	}
}

func requireModelEpochs(ctx context.Context, t *testing.T, collection client.Collection, epoch int) {
	t.Helper()
	for _, record := range []struct {
		group int
		name  string
	}{{10, "fence"}, {70, "fence"}, {10, "owner"}} {
		var doc struct{ Epoch int }
		require.NoError(t, collection.FindOne(ctx, bson.M{"group": record.group, "record": record.name}).Decode(&doc))
		require.Equal(t, epoch, doc.Epoch)
	}
}

func TestMongoDBModelFenceGranularity(t *testing.T) {
	fixture, cfg := newFencingTestFactory(t, "model_granularity")
	requireShardedTopology(t, fixture)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	t.Cleanup(cancel)
	factory := newModelMeasurementFactory(ctx, t, fixture, cfg)
	const (
		workflows = 32
		workers   = 8
		steps     = 32
		commits   = workers * steps
	)
	for _, layout := range []struct {
		name        string
		groups      int
		sharedFence bool
	}{
		{"shared_fence", workflows, true},
		{"bucket_fences", 4, false},
		{"workflow_fences", workflows, false},
	} {
		t.Run(layout.name, func(t *testing.T) {
			createModelCollection(ctx, t, fixture, cfg.DatabaseName, layout.name, "shard-rs-0")
			require.NoError(t, runAdminCommand(ctx, fixture, bson.D{
				{Key: "moveChunk", Value: cfg.DatabaseName + "." + layout.name},
				{Key: "find", Value: bson.D{{Key: "group", Value: 70}}},
				{Key: "to", Value: "shard-rs-0"}, {Key: "_waitForDelete", Value: true},
			}))
			collection := factory.database.Collection(layout.name)
			groupForWorkflow := func(workflowID int) int {
				bucket := workflowID % layout.groups
				if bucket < layout.groups/2 {
					return 10 + bucket
				}
				return 70 + bucket - layout.groups/2
			}
			fenceForWorkflow := func(workflowID int) int {
				if layout.sharedFence {
					return 10
				}
				return groupForWorkflow(workflowID)
			}
			// Seed through the workload client so its first snapshot observes the setup writes.
			seeding := collection
			for workflowID := range workflows {
				for _, kind := range []string{"state", "current"} {
					_, err := seeding.InsertOne(ctx, bson.M{
						"group": groupForWorkflow(workflowID), "record": fmt.Sprintf("%s_%d", kind, workflowID),
						"logical_shard": 17, "revision": 0, "payload": make([]byte, 1024),
					})
					require.NoError(t, err)
				}
			}
			fenceGroups := []int{10}
			if !layout.sharedFence {
				fenceGroups = make([]int, layout.groups)
				for bucket := range layout.groups {
					fenceGroups[bucket] = groupForWorkflow(bucket)
				}
			}
			for _, group := range fenceGroups {
				_, err := seeding.InsertOne(ctx, bson.M{"group": group, "record": "fence", "epoch": 100, "writes": 0})
				require.NoError(t, err)
			}
			_, err := seeding.InsertOne(ctx, bson.M{"group": 10, "record": "owner", "epoch": 100})
			require.NoError(t, err)
			_, seedErr := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
				var fence struct{ Epoch int }
				err := collection.FindOne(txCtx, bson.M{"group": 10, "record": "fence", "epoch": 100}).Decode(&fence)
				return nil, err
			})
			require.NoError(t, seedErr)
			mutate := func(txCtx context.Context, workflowID, taskID int) error {
				result, err := collection.UpdateOne(txCtx,
					bson.M{"group": fenceForWorkflow(workflowID), "record": "fence", "epoch": 100},
					bson.M{"$inc": bson.M{"writes": 1}})
				if err != nil {
					return err
				}
				if result.MatchedCount != 1 {
					return errors.New("model ownership fence did not match")
				}
				for _, kind := range []string{"state", "current"} {
					result, err := collection.UpdateOne(txCtx,
						bson.M{"group": groupForWorkflow(workflowID), "record": fmt.Sprintf("%s_%d", kind, workflowID)},
						bson.M{"$inc": bson.M{"revision": 1}})
					if err != nil {
						return err
					}
					if result.MatchedCount != 1 {
						return errors.New("model workflow record did not match")
					}
				}
				_, err = collection.InsertOne(txCtx, bson.M{
					"group": groupForWorkflow(workflowID), "record": fmt.Sprintf("task_%d", taskID),
					"logical_shard": 17, "kind": "task", "task_id": taskID,
				})
				return err
			}
			for phase, phaseName := range []string{"concentrated", "redistributed"} {
				if phase == 1 {
					require.NoError(t, runAdminCommand(ctx, fixture, bson.D{
						{Key: "moveChunk", Value: cfg.DatabaseName + "." + layout.name},
						{Key: "find", Value: bson.D{{Key: "group", Value: 70}}},
						{Key: "to", Value: "shard-rs-1"}, {Key: "_waitForDelete", Value: true},
					}))
				}
				for _, workflowID := range []int{0, workflows - 1} {
					group := groupForWorkflow(workflowID)
					target := "shard-rs-0"
					if phase == 1 && group >= 70 {
						target = "shard-rs-1"
					}
					require.Equal(t, target, explainTargetShard(ctx, t, fixture, layout.name, bson.D{{Key: "group", Value: group}}))
				}
				var attempts atomic.Int64
				latencies := make([]time.Duration, commits)
				outcomes := make(chan error, workers)
				start := make(chan struct{})
				phaseCtx, phaseCancel := context.WithCancel(ctx)
				var writers sync.WaitGroup
				writers.Add(workers)
				for workerID := range workers {
					go func() {
						defer writers.Done()
						<-start
						for step := range steps {
							workflowID := workerID + (step%(workflows/workers))*workers
							operation := workerID*steps + step
							began := time.Now()
							_, err := runModelTransaction(phaseCtx, factory, func(txCtx context.Context) (interface{}, error) {
								attempts.Add(1)
								return nil, mutate(txCtx, workflowID, phase*(commits+2)+operation)
							})
							if err != nil {
								phaseCancel()
								outcomes <- err
								return
							}
							latencies[operation] = time.Since(began)
						}
						outcomes <- nil
					}()
				}
				began := time.Now()
				close(start)
				writers.Wait()
				elapsed := time.Since(began)
				phaseCancel()
				close(outcomes)
				for err := range outcomes {
					require.NoError(t, err)
				}
				require.GreaterOrEqual(t, attempts.Load(), int64(commits))
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				t.Logf("%s groups=%d commits=%d attempts=%d elapsed=%s p50=%s p95=%s (local bounded-record probe, not Temporal capacity)",
					phaseName, layout.groups, commits, attempts.Load(), elapsed, latencies[commits/2], latencies[commits*95/100])
				for sample, workflowID := range []int{0, workflows - 1} {
					participants := 1
					if phase == 1 && layout.sharedFence && workflowID == workflows-1 {
						participants = 2
					}
					_, err := runMeasuredModelTransaction(ctx, t, factory, participants, func(txCtx context.Context) (interface{}, error) {
						return nil, mutate(txCtx, workflowID, phase*(commits+2)+commits+sample)
					})
					require.NoError(t, err)
				}
				for workflowID := range workflows {
					expectedRevision := (phase + 1) * steps / (workflows / workers)
					if workflowID == 0 || workflowID == workflows-1 {
						expectedRevision += phase + 1
					}
					for _, kind := range []string{"state", "current"} {
						var durable struct{ Revision int }
						require.NoError(t, collection.FindOne(ctx, bson.M{"group": groupForWorkflow(workflowID), "record": fmt.Sprintf("%s_%d", kind, workflowID)}).Decode(&durable))
						require.Equal(t, expectedRevision, durable.Revision)
					}
				}
				count, err := collection.CountDocuments(ctx, bson.M{"logical_shard": 17, "kind": "task"})
				require.NoError(t, err)
				require.Equal(t, int64((phase+1)*(commits+2)), count, "no lost or duplicated outbox writes after transaction retries")
				var writes int64
				for _, group := range fenceGroups {
					var fence struct {
						Epoch  int
						Writes int64
					}
					require.NoError(t, collection.FindOne(ctx, bson.M{"group": group, "record": "fence"}).Decode(&fence))
					require.Equal(t, 100, fence.Epoch)
					writes += fence.Writes
				}
				require.Equal(t, count, writes, "aborted callbacks must not leave fence increments")
			}
			abortTakeover := errors.New("abort before all fences are changed")
			takeover := func(txCtx context.Context, fail bool) error {
				for i, group := range fenceGroups {
					result, err := collection.UpdateOne(txCtx,
						bson.M{"group": group, "record": "fence", "epoch": 100},
						bson.M{"$set": bson.M{"epoch": 101}})
					if err != nil {
						return err
					}
					if result.MatchedCount != 1 {
						return errors.New("takeover fence did not match")
					}
					if fail && i == 0 {
						return abortTakeover
					}
				}
				result, err := collection.UpdateOne(txCtx,
					bson.M{"group": 10, "record": "owner", "epoch": 100},
					bson.M{"$set": bson.M{"epoch": 101}})
				if err != nil {
					return err
				}
				if result.MatchedCount != 1 {
					return errors.New("takeover publication did not match")
				}
				return nil
			}
			requireEpoch := func(epoch int) {
				for _, group := range fenceGroups {
					var fence struct{ Epoch int }
					require.NoError(t, collection.FindOne(ctx, bson.M{"group": group, "record": "fence"}).Decode(&fence))
					require.Equal(t, epoch, fence.Epoch)
				}
				var owner struct{ Epoch int }
				require.NoError(t, collection.FindOne(ctx, bson.M{"group": 10, "record": "owner"}).Decode(&owner))
				require.Equal(t, epoch, owner.Epoch)
			}
			_, err = runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
				return nil, takeover(txCtx, true)
			})
			require.ErrorIs(t, err, abortTakeover)
			requireEpoch(100)
			participants := 2
			if layout.sharedFence {
				participants = 1
			}
			began := time.Now()
			var takeoverAttempts int
			_, err = runMeasuredModelTransaction(ctx, t, factory, participants, func(txCtx context.Context) (interface{}, error) {
				takeoverAttempts++
				return nil, takeover(txCtx, false)
			})
			require.NoError(t, err)
			requireEpoch(101)
			t.Logf("takeover fences=%d plus publication participants=%d attempts=%d elapsed_with_log_checks=%s", len(fenceGroups), participants, takeoverAttempts, time.Since(began))
			for _, workflowID := range []int{0, workflows - 1} {
				_, err = runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
					return nil, mutate(txCtx, workflowID, 2*(commits+2)+workflowID)
				})
				require.ErrorContains(t, err, "model ownership fence did not match")
			}
			count, err := collection.CountDocuments(ctx, bson.M{"logical_shard": 17, "kind": "task"})
			require.NoError(t, err)
			require.Equal(t, int64(2*(commits+2)), count, "stale writers must not publish outbox records")
		})
	}
}

func modelGroupForBucket(bucket, buckets int) int {
	if bucket < buckets/2 {
		return 10 - bucket
	}
	return 70 + bucket - buckets/2
}

func TestMongoDBModelTaskFanout(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "model_task_fanout")
	requireShardedTopology(t, factory)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	t.Cleanup(cancel)
	const (
		taskCount = 256
		pageSize  = 17
	)
	type orderedTask struct {
		TaskID   int64 `bson:"task_id"`
		FireTime int64 `bson:"fire_time"`
	}
	for _, groups := range []int{4, 32, 128} {
		for _, strategy := range []string{"router_merge", "per_group_merge"} {
			t.Run(fmt.Sprintf("groups_%d/%s", groups, strategy), func(t *testing.T) {
				name := fmt.Sprintf("fanout_%d_%s", groups, strategy)
				createModelCollection(ctx, t, factory, cfg.DatabaseName, name, "shard-rs-0")
				collection := factory.database.Collection(name)
				for _, kind := range []string{"task", "timer"} {
					for _, prefix := range []bson.D{nil, {{Key: "group", Value: 1}}} {
						keys := append(prefix, bson.E{Key: "logical_shard", Value: 1}, bson.E{Key: "kind", Value: 1})
						if kind == "timer" {
							keys = append(keys, bson.E{Key: "fire_time", Value: 1})
						}
						keys = append(keys, bson.E{Key: "task_id", Value: 1})
						_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys})
						require.NoError(t, err)
					}
				}
				var records []interface{}
				for bucket := range groups {
					records = append(records, bson.M{"group": modelGroupForBucket(bucket, groups), "record": "state", "logical_shard": 17})
				}
				for id := 1; id <= taskCount; id++ {
					for _, kind := range []string{"task", "timer"} {
						records = append(records, bson.M{
							"group": modelGroupForBucket((id-1)%groups, groups), "record": fmt.Sprintf("%s_%d", kind, id),
							"logical_shard": 17, "kind": kind, "task_id": int64(id), "fire_time": int64(1000 + id%4),
						})
					}
				}
				records = append(records,
					bson.M{"group": 10, "record": "early_high_id", "logical_shard": 17, "kind": "timer", "task_id": int64(10000), "fire_time": int64(1000)},
					bson.M{"group": 70, "record": "exclusive_boundary", "logical_shard": 17, "kind": "timer", "task_id": int64(257), "fire_time": int64(1004)},
					bson.M{"group": 70, "record": "other_shard", "logical_shard": 18, "kind": "task", "task_id": int64(1)},
				)
				_, err := collection.InsertMany(ctx, records)
				require.NoError(t, err)
				oneGroup, err := collection.CountDocuments(ctx, bson.M{"group": 10, "logical_shard": 17, "kind": "task"})
				require.NoError(t, err)
				require.Equal(t, int64(taskCount/groups), oneGroup, "a single-group reader misses other groups' tasks")
				for _, control := range []struct {
					filter bson.M
					count  int64
				}{
					{bson.M{"logical_shard": 17, "kind": "timer", "fire_time": bson.M{"$gte": 1000}}, 258},
					{bson.M{"logical_shard": 17, "kind": "timer", "fire_time": bson.M{"$gte": 1000, "$lt": 1004}, "task_id": bson.M{"$lt": 257}}, 256},
				} {
					count, err := collection.CountDocuments(ctx, control.filter)
					require.NoError(t, err)
					require.Equal(t, control.count, count, "incorrect scheduled bounds must change membership")
				}
				for _, kind := range []string{"task", "timer"} {
					t.Run(kind, func(t *testing.T) {
						sortOrder := bson.D{{Key: "task_id", Value: 1}}
						if kind == "timer" {
							sortOrder = bson.D{{Key: "fire_time", Value: 1}, {Key: "task_id", Value: 1}}
						}
						var last orderedTask
						filterForPage := func() bson.M {
							filter := bson.M{"logical_shard": 17, "kind": kind}
							if kind == "timer" {
								filter["fire_time"] = bson.M{"$gte": 1000, "$lt": 1004}
								if last.TaskID != 0 {
									filter["$or"] = []bson.M{
										{"fire_time": bson.M{"$gt": last.FireTime}},
										{"fire_time": last.FireTime, "task_id": bson.M{"$gt": last.TaskID}},
									}
								}
							} else {
								filter["task_id"] = bson.M{"$gte": 1, "$lt": 257}
								if last.TaskID != 0 {
									filter["task_id"] = bson.M{"$gt": last.TaskID, "$lt": 257}
								}
							}
							return filter
						}
						base := filterForPage()
						require.Equal(t, 2, explainShardCount(ctx, t, factory, name, base))
						for _, bucket := range []int{0, groups - 1} {
							filter := filterForPage()
							filter["group"] = modelGroupForBucket(bucket, groups)
							require.Equal(t, 1, explainShardCount(ctx, t, factory, name, filter))
						}
						began := time.Now()
						var actual []orderedTask
						queries, fetched, pages := 0, 0, 0
						for {
							var page []orderedTask
							queryCount := 1
							if strategy == "per_group_merge" {
								queryCount = groups
							}
							for bucket := range queryCount {
								filter := filterForPage()
								if strategy == "per_group_merge" {
									filter["group"] = modelGroupForBucket(bucket, groups)
								}
								cursor, err := collection.Find(ctx, filter, options.Find().SetSort(sortOrder).SetLimit(pageSize+1))
								require.NoError(t, err)
								var result []orderedTask
								require.NoError(t, cursor.All(ctx, &result))
								page = append(page, result...)
								queries++
								fetched += len(result)
							}
							sort.Slice(page, func(i, j int) bool {
								if kind == "timer" && page[i].FireTime != page[j].FireTime {
									return page[i].FireTime < page[j].FireTime
								}
								return page[i].TaskID < page[j].TaskID
							})
							if len(page) > pageSize {
								page = page[:pageSize]
							}
							actual = append(actual, page...)
							pages++
							require.LessOrEqual(t, pages, taskCount, "pagination must make progress")
							if len(page) < pageSize {
								break
							}
							last = page[len(page)-1]
						}
						var expected []orderedTask
						if kind == "task" {
							for id := int64(1); id <= taskCount; id++ {
								expected = append(expected, orderedTask{TaskID: id, FireTime: 1000 + id%4})
							}
						} else {
							for fire := int64(1000); fire < 1004; fire++ {
								for id := int64(1); id <= taskCount; id++ {
									if 1000+id%4 == fire {
										expected = append(expected, orderedTask{TaskID: id, FireTime: fire})
									}
								}
								if fire == 1000 {
									expected = append(expected, orderedTask{TaskID: 10000, FireTime: 1000})
								}
							}
						}
						require.Equal(t, expected, actual, "all pages must preserve membership and global ordering")
						t.Logf("groups=%d strategy=%s kind=%s tasks=%d pages=%d Find_calls=%d fetched=%d elapsed=%s (local query probe)", groups, strategy, kind, len(actual), pages, queries, fetched, time.Since(began))
						ack := bson.M{"logical_shard": 17, "kind": kind, "task_id": bson.M{"$gte": 1, "$lt": 129}}
						if kind == "timer" {
							ack = bson.M{"logical_shard": 17, "kind": kind, "fire_time": bson.M{"$gte": 1000, "$lt": 1002}}
						}
						var deleted int64
						ackCalls := 1
						if strategy == "per_group_merge" {
							ackCalls = groups
						}
						for bucket := range ackCalls {
							if strategy == "per_group_merge" {
								ack["group"] = modelGroupForBucket(bucket, groups)
							}
							result, err := collection.DeleteMany(ctx, ack)
							require.NoError(t, err)
							deleted += result.DeletedCount
						}
						expectedDeleted := int64(128)
						if kind == "timer" {
							expectedDeleted++
						}
						require.Equal(t, expectedDeleted, deleted)
						t.Logf("groups=%d strategy=%s kind=%s DeleteMany_calls=%d acknowledged=%d", groups, strategy, kind, ackCalls, deleted)
						cursor, err := collection.Find(ctx, bson.M{"logical_shard": 17, "kind": kind}, options.Find().SetSort(sortOrder))
						require.NoError(t, err)
						var remaining []orderedTask
						require.NoError(t, cursor.All(ctx, &remaining))
						var expectedRemaining []orderedTask
						for _, task := range expected {
							if (kind == "task" && task.TaskID >= 129) || (kind == "timer" && task.FireTime >= 1002) {
								expectedRemaining = append(expectedRemaining, task)
							}
						}
						if kind == "timer" {
							expectedRemaining = append(expectedRemaining, orderedTask{TaskID: 257, FireTime: 1004})
						}
						require.Equal(t, expectedRemaining, remaining, "exclusive acknowledgement boundary must survive")
					})
				}
				for _, protection := range []struct {
					filter bson.M
					count  int64
				}{{bson.M{"logical_shard": 18}, 1}, {bson.M{"record": "state"}, int64(groups)}} {
					count, err := collection.CountDocuments(ctx, protection.filter)
					require.NoError(t, err)
					require.Equal(t, protection.count, count, "acknowledgement must preserve unrelated records")
				}
			})
		}
	}
}
