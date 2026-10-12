//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

func TestMongoDBModelAppendBeforeStateCommit(t *testing.T) {
	factory, _ := newFencingTestFactory(t, "model_append_protocol")
	requireShardedTopology(t, factory)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	storeValue, err := factory.NewExecutionStore()
	require.NoError(t, err)
	store, ok := storeValue.(*executionStore)
	require.True(t, ok)
	serializer := serialization.NewSerializer()
	manager := persistence.NewExecutionManager(store, serializer, nil, factory.logger,
		dynamicconfig.GetIntPropertyFn(4*1024*1024), dynamicconfig.GetBoolPropertyFn(false))
	const (
		shardID      = int32(17)
		firstTxn     = int64(100<<20 + 11)
		secondTxn    = int64(100<<20 + 22)
		abandonedTxn = int64(100<<20 + 33)
		retryTxn     = int64(100<<20 + 44)
	)
	branchID := "branch"
	token, err := store.GetHistoryBranchUtil().NewHistoryBranch("namespace", "workflow", "run", "tree", &branchID, nil, 0, 0, 0)
	require.NoError(t, err)
	branch, err := store.GetHistoryBranchUtil().ParseHistoryBranchInfo(token)
	require.NoError(t, err)
	treeInfo, err := serializer.HistoryTreeInfoToBlob(&persistencespb.HistoryTreeInfo{BranchInfo: branch, BranchToken: token})
	require.NoError(t, err)
	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)
	_, err = shardStore.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID, LifecycleContext: ctx,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 100, persistence.NewDataBlob([]byte("shard"), enumspb.ENCODING_TYPE_PROTO3.String()), nil
		},
	})
	require.NoError(t, err)
	info := &persistencespb.WorkflowExecutionInfo{LastFirstEventId: 2, LastFirstEventTxnId: secondTxn}
	state := &persistencespb.WorkflowExecutionState{RunId: "run", CreateRequestId: "request", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	event := func(id int64, marker string) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventId: id, Version: 1, EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
			Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{
				MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{MarkerName: marker},
			},
		}
	}
	var batches []*persistence.InternalAppendHistoryNodesRequest
	for _, node := range []struct{ id, txn, prev int64 }{{1, firstTxn, 0}, {2, secondTxn, firstTxn}} {
		blob, err := serializer.SerializeEvents([]*historypb.HistoryEvent{event(node.id, "committed")})
		require.NoError(t, err)
		batches = append(batches, &persistence.InternalAppendHistoryNodesRequest{
			ShardID: shardID, BranchToken: token, BranchInfo: branch, IsNewBranch: node.id == 1,
			TreeInfo: treeInfo,
			Node:     persistence.InternalHistoryNode{NodeID: node.id, TransactionID: node.txn, PrevTransactionID: node.prev, Events: blob},
		})
	}
	_, err = store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: shardID, RangeID: 100, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "namespace", WorkflowID: "workflow", RunID: "run", ExecutionInfo: info, ExecutionState: state, NextEventID: 3, DBRecordVersion: 1,
		},
		NewWorkflowNewEvents: batches,
	})
	require.NoError(t, err)
	orphanBlob, err := serializer.SerializeEvents([]*historypb.HistoryEvent{event(3, "abandoned")})
	require.NoError(t, err)
	orphan := &persistence.InternalAppendHistoryNodesRequest{
		ShardID: shardID, BranchToken: token, BranchInfo: branch,
		Node: persistence.InternalHistoryNode{NodeID: 3, TransactionID: abandonedTxn, PrevTransactionID: secondTxn, Events: orphanBlob},
	}
	failingExecutions := &failOnceCollection{Collection: factory.database.Collection(collectionHistoryRecords)}
	store.executionsCol = failingExecutions
	failedState := &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: shardID, RangeID: 100, Mode: persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{orphan},
		UpdateWorkflowMutation: persistence.InternalWorkflowMutation{
			NamespaceID: "namespace", WorkflowID: "workflow", RunID: "run", ExecutionState: state,
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{LastFirstEventId: 3, LastFirstEventTxnId: abandonedTxn},
			NextEventID:   4, DBRecordVersion: 2, Condition: 3,
		},
	}
	failingExecutions.failNext.Store(true)
	require.ErrorContains(t, store.UpdateWorkflowExecution(ctx, failedState), "injected permanent failure")
	count, err := factory.database.Collection(collectionHistoryNodes).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "atomic baseline must not retain history after the state write fails")

	// Model only the proposed ordering change using the real store and history manager.
	require.NoError(t, store.AppendHistoryNodes(ctx, orphan))
	failedState.UpdateWorkflowNewEvents = nil
	failingExecutions.failNext.Store(true)
	require.ErrorContains(t, store.UpdateWorkflowExecution(ctx, failedState), "injected permanent failure")
	mutable, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{ShardID: shardID, NamespaceID: "namespace", WorkflowID: "workflow", RunID: "run"})
	require.NoError(t, err)
	require.Equal(t, int64(1), mutable.State.DBRecordVersion)
	require.Equal(t, int64(3), mutable.State.NextEventID)
	committedInfo, err := serializer.WorkflowExecutionInfoFromBlob(mutable.State.ExecutionInfo)
	require.NoError(t, err)
	require.Equal(t, secondTxn, committedInfo.LastFirstEventTxnId)
	var duplicate *persistence.ConditionFailedError
	require.ErrorAs(t, store.AppendHistoryNodes(ctx, orphan), &duplicate, "an ambiguous append retry is not automatically idempotent")

	forward := &persistence.ReadHistoryBranchRequest{ShardID: shardID, BranchToken: token, MinEventID: 1, MaxEventID: 3, PageSize: 10}
	read, err := manager.ReadHistoryBranch(ctx, forward)
	require.NoError(t, err)
	require.Len(t, read.HistoryEvents, 2)
	for _, item := range read.HistoryEvents {
		require.Equal(t, "committed", item.GetMarkerRecordedEventAttributes().GetMarkerName(), "committed event boundary excludes an abandoned future batch")
	}
	reverse, err := manager.ReadHistoryBranchReverse(ctx, &persistence.ReadHistoryBranchReverseRequest{
		ShardID: shardID, BranchToken: token, MaxEventID: 3, PageSize: 10, LastFirstTransactionID: committedInfo.LastFirstEventTxnId,
	})
	require.NoError(t, err)
	require.Len(t, reverse.HistoryEvents, 2)
	for _, event := range reverse.HistoryEvents {
		require.Equal(t, int64(1), event.Version, "anchored reverse read follows only the committed chain")
		require.Equal(t, "committed", event.GetMarkerRecordedEventAttributes().GetMarkerName())
	}
	retryBlob, err := serializer.SerializeEvents([]*historypb.HistoryEvent{event(3, "committed retry")})
	require.NoError(t, err)
	retry := *orphan
	retry.Node.TransactionID = retryTxn
	retry.Node.Events = retryBlob
	require.NoError(t, store.AppendHistoryNodes(ctx, &retry))
	failedState.UpdateWorkflowMutation.ExecutionInfo.LastFirstEventTxnId = retryTxn
	require.NoError(t, store.UpdateWorkflowExecution(ctx, failedState))
	mutable, err = store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{ShardID: shardID, NamespaceID: "namespace", WorkflowID: "workflow", RunID: "run"})
	require.NoError(t, err)
	require.Equal(t, int64(2), mutable.State.DBRecordVersion)
	require.Equal(t, int64(4), mutable.State.NextEventID)
	committedInfo, err = serializer.WorkflowExecutionInfoFromBlob(mutable.State.ExecutionInfo)
	require.NoError(t, err)
	require.Equal(t, retryTxn, committedInfo.LastFirstEventTxnId)
	forward.MaxEventID = 4
	read, err = manager.ReadHistoryBranch(ctx, forward)
	require.NoError(t, err)
	require.Len(t, read.HistoryEvents, 3)
	require.Equal(t, "committed retry", read.HistoryEvents[2].GetMarkerRecordedEventAttributes().GetMarkerName())
	reverse, err = manager.ReadHistoryBranchReverse(ctx, &persistence.ReadHistoryBranchReverseRequest{
		ShardID: shardID, BranchToken: token, MaxEventID: 4, PageSize: 10, LastFirstTransactionID: retryTxn,
	})
	require.NoError(t, err)
	require.Len(t, reverse.HistoryEvents, 3)
	require.Equal(t, "committed retry", reverse.HistoryEvents[0].GetMarkerRecordedEventAttributes().GetMarkerName())
	_, err = manager.TrimHistoryBranch(ctx, &persistence.TrimHistoryBranchRequest{
		ShardID: shardID, BranchToken: token, NodeID: 3, TransactionID: retryTxn,
	})
	require.NoError(t, err)
	count, err = factory.database.Collection(collectionHistoryNodes).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(4), count, "trim repairs newer conflicting versions, not older abandoned appends")
	require.NoError(t, manager.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{ShardID: shardID, BranchToken: token}))
	for _, name := range []string{collectionHistoryNodes, collectionHistoryBranches} {
		count, err := factory.database.Collection(name).CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Zero(t, count, "branch deletion must reclaim committed and abandoned records")
	}
	t.Log("append-before-state probe: bounded/anchored readers and newer transaction retry work; exact append retry fails and older orphan survives trim. Keep atomic commit until a complete retry/cleanup protocol is qualified.")
}

func TestMongoDBModelHotHistoryPlacement(t *testing.T) {
	fixture, cfg := newFencingTestFactory(t, "model_hot_history")
	requireShardedTopology(t, fixture)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	t.Cleanup(cancel)
	factory := newModelMeasurementFactory(ctx, t, fixture, cfg)
	serializer := serialization.NewSerializer()
	const (
		nodes       = 128
		segmentSize = 16
		replayPage  = 7
		firstTxn    = int64(100 << 20)
		childTxn    = int64(100<<20 + 1000)
		forkNode    = 65
		childLast   = 80
	)
	type historyRecord struct {
		NodeID    int64        `bson:"node_id"`
		TxnID     int64        `bson:"txn_id"`
		PrevTxnID int64        `bson:"prev_txn_id"`
		Events    *dataBlobDoc `bson:"events"`
	}
	for _, layout := range []string{"tree_local", "tree_remote", "segmented"} {
		t.Run(layout, func(t *testing.T) {
			createModelCollection(ctx, t, fixture, cfg.DatabaseName, layout, "shard-rs-0")
			collection := factory.database.Collection(layout)
			for _, prefix := range []bson.D{nil, {{Key: "group", Value: 1}}} {
				keys := append(prefix, bson.E{Key: "kind", Value: 1}, bson.E{Key: "tree_id", Value: 1}, bson.E{Key: "branch_id", Value: 1}, bson.E{Key: "node_id", Value: 1}, bson.E{Key: "txn_id", Value: -1})
				_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys})
				require.NoError(t, err)
			}
			groupForNode := func(id int) int {
				switch layout {
				case "tree_remote":
					return 70
				case "segmented":
					return modelGroupForBucket((id-1)/segmentSize, nodes/segmentSize)
				default:
					return 10
				}
			}
			_, err := collection.InsertMany(ctx, []interface{}{
				bson.M{"group": 10, "record": "fence", "epoch": 100, "writes": 0},
				bson.M{"group": 10, "record": "state_main", "next_node": 1, "last_txn": int64(0)},
				bson.M{"group": 70, "record": "cold_history", "kind": "history", "tree_id": "cold", "branch_id": "main", "node_id": 1},
			})
			require.NoError(t, err)
			appendNode := func(txCtx context.Context, branch string, id int, txn, prev int64, abort error) error {
				result, err := collection.UpdateOne(txCtx, bson.M{"group": 10, "record": "fence", "epoch": 100}, bson.M{"$inc": bson.M{"writes": 1}})
				if err != nil {
					return err
				}
				if result.MatchedCount != 1 {
					return errors.New("hot-history fence did not match")
				}
				blob, err := serializer.SerializeEvents([]*historypb.HistoryEvent{{
					EventId: int64(id), Version: 1, EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
					Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
						MarkerName: branch, Details: map[string]*commonpb.Payloads{"payload": {Payloads: []*commonpb.Payload{{Data: make([]byte, 1024)}}}},
					}},
				}})
				if err != nil {
					return err
				}
				_, err = collection.InsertOne(txCtx, bson.M{
					"group": groupForNode(id), "record": fmt.Sprintf("%s_%d", branch, id), "kind": "history", "tree_id": "hot", "branch_id": branch,
					"node_id": int64(id), "txn_id": txn, "prev_txn_id": prev, "events": dataBlobToDoc(blob),
				})
				if err != nil {
					return err
				}
				result, err = collection.UpdateOne(txCtx,
					bson.M{"group": 10, "record": "state_" + branch, "next_node": id, "last_txn": prev},
					bson.M{"$set": bson.M{"next_node": id + 1, "last_txn": txn}})
				if err != nil {
					return err
				}
				if result.MatchedCount != 1 {
					return errors.New("hot-history state did not match committed tail")
				}
				return abort
			}
			checkState := func(branch string, next int, txn int64) {
				var state struct {
					NextNode int   `bson:"next_node"`
					LastTxn  int64 `bson:"last_txn"`
				}
				require.NoError(t, collection.FindOne(ctx, bson.M{"group": 10, "record": "state_" + branch}).Decode(&state))
				require.Equal(t, next, state.NextNode)
				require.Equal(t, txn, state.LastTxn)
			}
			var attempts int
			began := time.Now()
			for id := 1; id <= nodes; id++ {
				prev := firstTxn + int64(id-1)
				if id == 1 {
					prev = 0
				}
				if id == forkNode {
					abort := errors.New("abort history and state at segment boundary")
					_, err := runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
						return nil, appendNode(txCtx, "main", id, firstTxn+int64(id), prev, abort)
					})
					require.ErrorIs(t, err, abort)
					checkState("main", id, prev)
					count, err := collection.CountDocuments(ctx, bson.M{"tree_id": "hot"})
					require.NoError(t, err)
					require.Equal(t, int64(id-1), count, "failed commit must retain neither history nor a new state tail")
					var fence struct{ Writes int }
					require.NoError(t, collection.FindOne(ctx, bson.M{"group": 10, "record": "fence"}).Decode(&fence))
					require.Equal(t, id-1, fence.Writes)
				}
				write := func(txCtx context.Context) (interface{}, error) {
					attempts++
					return nil, appendNode(txCtx, "main", id, firstTxn+int64(id), prev, nil)
				}
				if id == 1 || id == 17 || id == forkNode || id == nodes {
					participants := 1
					if groupForNode(id) >= 50 {
						participants = 2
					}
					_, err = runMeasuredModelTransaction(ctx, t, factory, participants, write)
					require.NoError(t, err)
				} else {
					_, err = runModelTransaction(ctx, factory, write)
					require.NoError(t, err)
				}
			}
			t.Logf("layout=%s atomic_appends=%d attempts=%d elapsed_with_sample_log_checks=%s (local sequential probe)", layout, nodes, attempts, time.Since(began))
			checkState("main", nodes+1, firstTxn+nodes)
			for _, half := range []struct {
				filter bson.M
				count  int64
			}{{bson.M{"$lt": 50}, 64}, {bson.M{"$gte": 50}, 64}} {
				var expected int64
				switch layout {
				case "tree_local":
					if half.filter["$lt"] != nil {
						expected = nodes
					} else {
						expected = 0
					}
				case "tree_remote":
					if half.filter["$gte"] != nil {
						expected = nodes
					} else {
						expected = 0
					}
				default:
					expected = half.count
				}
				count, err := collection.CountDocuments(ctx, bson.M{"kind": "history", "tree_id": "hot", "branch_id": "main", "group": half.filter})
				require.NoError(t, err)
				require.Equal(t, expected, count)
			}
			if layout == "segmented" {
				count, err := collection.CountDocuments(ctx, bson.M{"group": 10, "kind": "history", "tree_id": "hot", "branch_id": "main"})
				require.NoError(t, err)
				require.Equal(t, int64(segmentSize), count, "a tree-local reader would return only one segment")
			}
			readRange := func(branch string, minNode, maxNode int, reverse, routed bool) []historyRecord {
				var result []historyRecord
				var ranges [][2]int
				if routed && layout == "segmented" {
					for lower := minNode; lower < maxNode; {
						upper := min(maxNode, ((lower-1)/segmentSize+1)*segmentSize+1)
						ranges = append(ranges, [2]int{lower, upper})
						lower = upper
					}
				} else {
					ranges = append(ranges, [2]int{minNode, maxNode})
				}
				if reverse {
					for i, j := 0, len(ranges)-1; i < j; i, j = i+1, j-1 {
						ranges[i], ranges[j] = ranges[j], ranges[i]
					}
				}
				calls := 0
				started := time.Now()
				for _, bounds := range ranges {
					last := 0
					for {
						nodeRange := bson.M{"$gte": bounds[0], "$lt": bounds[1]}
						order := 1
						if reverse {
							order = -1
						}
						if last != 0 {
							if reverse {
								nodeRange["$lt"] = last
							} else {
								nodeRange["$gt"] = last
							}
						}
						filter := bson.M{"kind": "history", "tree_id": "hot", "branch_id": branch, "node_id": nodeRange}
						participants := 2
						if routed {
							filter["group"] = groupForNode(bounds[0])
							participants = 1
						}
						if last == 0 {
							require.Equal(t, participants, explainShardCount(ctx, t, fixture, layout, filter))
							if routed {
								target := "shard-rs-0"
								if groupForNode(bounds[0]) >= 50 {
									target = "shard-rs-1"
								}
								require.Equal(t, target, explainTargetShard(ctx, t, fixture, layout, filter))
							}
						}
						cursor, err := collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "node_id", Value: order}, {Key: "txn_id", Value: -1}}).SetLimit(replayPage))
						require.NoError(t, err)
						var page []historyRecord
						require.NoError(t, cursor.All(ctx, &page))
						result = append(result, page...)
						calls++
						require.LessOrEqual(t, calls, nodes, "replay pagination must make progress")
						if len(page) < replayPage {
							break
						}
						last = int(page[len(page)-1].NodeID)
					}
				}
				t.Logf("layout=%s branch=%s reverse=%t routed=%t nodes=%d Find_calls=%d elapsed_with_explain=%s", layout, branch, reverse, routed, len(result), calls, time.Since(started))
				return result
			}
			checkReplay := func(records []historyRecord, first, last int, reverse bool, branch string) {
				require.Len(t, records, last-first+1)
				for offset, record := range records {
					id := first + offset
					if reverse {
						id = last - offset
					}
					require.Equal(t, int64(id), record.NodeID)
					expectedTxn, previous := firstTxn+int64(id), firstTxn+int64(id-1)
					if id == 1 {
						previous = 0
					}
					if branch == "child" {
						expectedTxn = childTxn + int64(id)
						if id > forkNode {
							previous = childTxn + int64(id-1)
						}
					}
					require.Equal(t, expectedTxn, record.TxnID)
					require.Equal(t, previous, record.PrevTxnID)
					events, err := serializer.DeserializeEvents(dataBlobDocToBlob(record.Events))
					require.NoError(t, err)
					require.Len(t, events, 1)
					require.Equal(t, int64(id), events[0].EventId)
					marker := events[0].GetMarkerRecordedEventAttributes()
					require.Equal(t, branch, marker.GetMarkerName())
					require.Len(t, marker.GetDetails()["payload"].GetPayloads()[0].GetData(), 1024)
				}
			}
			for _, routed := range []bool{false, true} {
				for _, reverse := range []bool{false, true} {
					checkReplay(readRange("main", 1, nodes+1, reverse, routed), 1, nodes, reverse, "main")
				}
			}
			_, err = collection.InsertOne(ctx, bson.M{"group": 10, "record": "state_child", "next_node": forkNode, "last_txn": firstTxn + forkNode - 1})
			require.NoError(t, err)
			for id := forkNode; id <= childLast; id++ {
				prev := childTxn + int64(id-1)
				if id == forkNode {
					prev = firstTxn + forkNode - 1
				}
				_, err = runModelTransaction(ctx, factory, func(txCtx context.Context) (interface{}, error) {
					return nil, appendNode(txCtx, "child", id, childTxn+int64(id), prev, nil)
				})
				require.NoError(t, err)
			}
			checkState("child", childLast+1, childTxn+childLast)
			checkState("main", nodes+1, firstTxn+nodes)
			for _, reverse := range []bool{false, true} {
				parent := readRange("main", 1, forkNode, reverse, true)
				child := readRange("child", forkNode, childLast+1, reverse, true)
				checkReplay(parent, 1, forkNode-1, reverse, "main")
				checkReplay(child, forkNode, childLast, reverse, "child")
			}
			result, err := collection.DeleteMany(ctx, bson.M{"kind": "history", "tree_id": "hot", "branch_id": "child"})
			require.NoError(t, err)
			require.Equal(t, int64(childLast-forkNode+1), result.DeletedCount)
			for _, protection := range []struct {
				filter bson.M
				count  int64
			}{{bson.M{"kind": "history", "tree_id": "hot", "branch_id": "main"}, nodes}, {bson.M{"tree_id": "cold"}, 1}, {bson.M{"record": "fence"}, 1}} {
				count, err := collection.CountDocuments(ctx, protection.filter)
				require.NoError(t, err)
				require.Equal(t, protection.count, count)
			}
		})
	}
}
