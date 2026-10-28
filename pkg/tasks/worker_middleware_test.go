package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_DropStaleReplicationTasks(t *testing.T) {
	replID := entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        "user",
		FromStorage: "main",
		ToStorage:   "follower",
	})
	payloadWithGeneration := func(generation int64) []byte {
		payload := ObjectSyncPayload{}
		payload.SetReplicationID(replID, generation)
		bytes, err := json.Marshal(payload)
		require.NoError(t, err)
		return bytes
	}
	tests := []struct {
		generationErr     error
		name              string
		queue             string
		payload           []byte
		currentGeneration int64
		wantProcessed     bool
		wantGenerationGet bool
		wantRetry         bool
		wantArchived      bool
	}{
		{
			name:              "current generation",
			queue:             EventMigrationQueue(replID),
			payload:           payloadWithGeneration(2),
			currentGeneration: 2,
			wantProcessed:     true,
			wantGenerationGet: true,
		},
		{
			name:              "deleted replication",
			queue:             EventMigrationQueue(replID),
			payload:           payloadWithGeneration(1),
			currentGeneration: 2,
			wantGenerationGet: true,
		},
		{
			name:              "previous replication with the same id",
			queue:             InitMigrationCopyQueue(replID),
			payload:           payloadWithGeneration(1),
			currentGeneration: 3,
			wantGenerationGet: true,
		},
		{
			name:              "legacy task of legacy replication",
			queue:             InitMigrationListQueue(replID),
			payload:           []byte(fmt.Sprintf(`{"ID":%q}`, replID.AsString())),
			currentGeneration: 0,
			wantProcessed:     true,
			wantGenerationGet: true,
		},
		{
			name:              "legacy task of replication created after upgrade",
			queue:             EventMigrationQueue(replID),
			payload:           []byte(fmt.Sprintf(`{"ID":%q}`, replID.AsString())),
			currentGeneration: 1,
			wantGenerationGet: true,
		},
		{
			name:              "not replication queue",
			queue:             string(QueueDiffFix) + ":test",
			payload:           payloadWithGeneration(0),
			currentGeneration: 2,
			wantProcessed:     true,
		},
		{
			name:              "generation get error",
			queue:             EventMigrationQueue(replID),
			payload:           payloadWithGeneration(2),
			generationErr:     errors.New("redis is down"),
			wantGenerationGet: true,
			wantRetry:         true,
		},
		{
			name:         "unparseable payload",
			queue:        EventMigrationQueue(replID),
			payload:      []byte("not json"),
			wantArchived: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			ctx := t.Context()
			c := testutil.SetupRedis(t)
			client := asynq.NewClientFromRedisClient(c)
			defer client.Close()
			inspector := asynq.NewInspectorFromRedisClient(c)
			defer inspector.Close()

			var mu sync.Mutex
			var requestedIDs []entity.UniversalReplicationID
			currentGeneration := func(_ context.Context, id entity.UniversalReplicationID) (int64, error) {
				mu.Lock()
				defer mu.Unlock()
				requestedIDs = append(requestedIDs, id)
				return tt.currentGeneration, tt.generationErr
			}
			var processed atomic.Bool
			handler := DropStaleReplicationTasks(currentGeneration)(asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
				processed.Store(true)
				return nil
			}))
			stop := registerHandler(t, ctx, c, tt.queue, handler.ProcessTask)
			defer stop()

			_, err := client.EnqueueContext(ctx, asynq.NewTask(TypeObjectSync, tt.payload), asynq.Queue(tt.queue))
			r.NoError(err)

			wantRetry, wantArchived := 0, 0
			if tt.wantRetry {
				wantRetry = 1
			}
			if tt.wantArchived {
				wantArchived = 1
			}
			r.Eventually(func() bool {
				info, err := inspector.GetQueueInfo(tt.queue)
				return err == nil && info.Size == wantRetry+wantArchived && info.Retry == wantRetry && info.Archived == wantArchived
			}, 5*time.Second, 10*time.Millisecond, "task reaches expected state")
			r.Equal(tt.wantProcessed, processed.Load())

			mu.Lock()
			defer mu.Unlock()
			if tt.wantGenerationGet {
				r.Equal([]entity.UniversalReplicationID{replID}, requestedIDs)
			} else {
				r.Empty(requestedIDs)
			}
		})
	}
}
