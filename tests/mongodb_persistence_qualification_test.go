//go:build integration

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests/testcore"
)

const (
	mongoDBRestartWorkflowCount  = 32
	mongoDBQualificationActivity = "mongodb-qualification-activity"
)

type MongoDBPersistenceQualificationSuite struct {
	testcore.FunctionalTestBase
	activityStarted   chan struct{}
	releaseActivities chan struct{}
}

func TestMongoDBPersistenceQualificationSuite(t *testing.T) {
	if !testcore.UseMongoDBPersistence() {
		t.Skip("MongoDB persistence qualification requires -persistenceType nosql -persistenceDriver mongodb")
	}
	suite.Run(t, new(MongoDBPersistenceQualificationSuite))
}

func (s *MongoDBPersistenceQualificationSuite) SetupSuite() {
	s.SetupSuiteWithCluster()
}

func (s *MongoDBPersistenceQualificationSuite) TestWorkflowsCompleteAcrossHistoryAndMatchingRestarts() {
	s.activityStarted = make(chan struct{}, mongoDBRestartWorkflowCount)
	s.releaseActivities = make(chan struct{})
	s.SdkWorker().RegisterWorkflow(mongoDBQualificationWorkflow)
	s.SdkWorker().RegisterActivityWithOptions(
		s.runMongoDBQualificationActivity,
		activity.RegisterOptions{Name: mongoDBQualificationActivity},
	)

	runs := make([]client.WorkflowRun, 0, mongoDBRestartWorkflowCount)
	for workflowIndex := range mongoDBRestartWorkflowCount {
		run, err := s.SdkClient().ExecuteWorkflow(
			testcore.NewContext(),
			client.StartWorkflowOptions{
				ID:        fmt.Sprintf("mongodb-restart-qualification-%d", workflowIndex),
				TaskQueue: s.TaskQueue(),
			},
			mongoDBQualificationWorkflow,
			workflowIndex,
		)
		s.Require().NoError(err)
		runs = append(runs, run)
	}

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for range mongoDBRestartWorkflowCount {
		select {
		case <-s.activityStarted:
		case <-deadline.C:
			s.T().Fatal("workflows did not reach the activity boundary")
		}
	}

	host := s.GetTestCluster().Host()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	s.Require().NoError(host.StopService(stopCtx, primitives.HistoryService))
	s.Require().NoError(host.StopService(stopCtx, primitives.MatchingService))
	stopCancel()

	close(s.releaseActivities)
	s.Require().NoError(host.StartService(primitives.HistoryService))
	s.Require().NoError(host.StartService(primitives.MatchingService))

	resultCtx, resultCancel := context.WithTimeout(context.Background(), time.Minute)
	defer resultCancel()
	for workflowIndex, run := range runs {
		var result int
		s.Require().NoError(run.Get(resultCtx, &result))
		s.Equal(workflowIndex, result)
	}
}

func (s *MongoDBPersistenceQualificationSuite) runMongoDBQualificationActivity(
	ctx context.Context,
	value int,
) (int, error) {
	s.activityStarted <- struct{}{}
	select {
	case <-s.releaseActivities:
		return value, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func mongoDBQualificationWorkflow(ctx workflow.Context, value int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Minute})
	var result int
	err := workflow.ExecuteActivity(ctx, mongoDBQualificationActivity, value).Get(ctx, &result)
	return result, err
}
