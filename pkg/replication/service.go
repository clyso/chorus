package replication

import (
	"context"
	"errors"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

type Service interface {
	Replicate(ctx context.Context, task tasks.SyncTask) error
}

func New(taskClient *asynq.Client, versionSvc meta.VersionService, policySvc policy.Service) Service {
	return &svc{
		taskClient: taskClient,
		versionSvc: versionSvc,
		policySvc:  policySvc,
	}
}

type svc struct {
	taskClient *asynq.Client
	versionSvc meta.VersionService
	policySvc  policy.Service
}

func (s *svc) Replicate(ctx context.Context, task tasks.SyncTask) error {
	if task == nil {
		zerolog.Ctx(ctx).Info().Msg("replication task is nil")
		return nil
	}
	zerolog.Ctx(ctx).Debug().Msg("creating replication task")
	task.InitDate()
	if task.GetFrom() == "" {
		storage := xctx.GetStorage(ctx)
		zerolog.Ctx(ctx).Warn().Msgf("replication task from storage not set, using storage from context: %s", storage)
		task.SetFrom(storage)
	}

	// 1. find repl rule(-s)
	replTo, err := s.getDestinations(ctx, task)
	if err != nil {
		if errors.Is(err, dom.ErrPolicy) {
			zerolog.Ctx(ctx).Info().Err(err).Msg("skip Replicate: replication is not configured")
			return nil
		}
		return err
	}
	if len(replTo) == 0 {
		// should be not possible
		zerolog.Ctx(ctx).Warn().Err(err).Msg("skip Replicate: followers are empty")
		return nil
	}
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)

	switch t := task.(type) {
	case *tasks.BucketCreatePayload:
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return err
			}
		}
	case *tasks.BucketDeletePayload:
		err = s.versionSvc.DeleteBucketAll(ctx, t.Bucket)
		if err != nil {
			return err
		}

		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.BucketSyncACLPayload:
		_, err = s.versionSvc.IncrementBucketACL(ctx, t.Bucket, t.FromStorage)
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.BucketSyncTagsPayload:
		_, err = s.versionSvc.IncrementBucketTags(ctx, t.Bucket, t.FromStorage)
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjectSyncPayload:
		_, err = s.versionSvc.IncrementObj(ctx, t.Object, t.FromStorage)
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjectDeletePayload:
		err = s.versionSvc.DeleteObjAll(ctx, t.Object)
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjSyncACLPayload:
		_, err = s.versionSvc.IncrementACL(ctx, t.Object, t.FromStorage)
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjSyncTagsPayload:
		_, err = s.versionSvc.IncrementTags(ctx, t.Object, t.FromStorage)
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to)
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetTo(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	default:
		return fmt.Errorf("%w: unsupported replication task type %T", dom.ErrInternal, task)
	}
	return nil
}

func (s *svc) getDestinations(ctx context.Context, task tasks.SyncTask) (map[string]tasks.Priority, error) {
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	policy, err := s.getReplicationPolicy(ctx, task)
	if err != nil {
		return nil, err
	}
	if policy.From == task.GetFrom() {
		return policy.To, nil
	}
	// Policy source storage is different from task source storage.
	// This only possible if switch is in progress, and we got CompleteMultipartUpload request.
	if _, ok := task.(*tasks.ObjectSyncPayload); !ok || (xctx.GetMethod(ctx) != s3.UndefinedMethod && xctx.GetMethod(ctx) != s3.CompleteMultipartUpload) {
		zerolog.Ctx(ctx).Warn().Msgf("routing policy from %q is different from task %q", policy.From, task.GetFrom())
	}

	replSwitch, err := s.policySvc.GetReplicationSwitch(ctx, user, bucket)
	if err != nil {
		return nil, fmt.Errorf("%w: no replication switch for replication task with invalid from storage", err)
	}
	if replSwitch.OldMain != task.GetFrom() {
		return nil, fmt.Errorf("%w: replication swithc OldMain %s not match with task from storage %s", dom.ErrInternal, replSwitch.OldMain, task.GetFrom())
	}

	return replSwitch.GetOldFollowers(), nil
}

func (s *svc) getReplicationPolicy(ctx context.Context, task tasks.SyncTask) (policy.ReplicationPolicies, error) {
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	bucketPolicy, err := s.policySvc.GetBucketReplicationPolicies(ctx, user, bucket)
	if err == nil {
		return bucketPolicy, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return policy.ReplicationPolicies{}, err
	}
	// policy not found. Create new bucket policy from user policy only for CreateBucket method
	if _, ok := task.(*tasks.BucketCreatePayload); !ok {
		return policy.ReplicationPolicies{}, fmt.Errorf("%w: replicatoin policy not configured: %v", dom.ErrPolicy, err)
	}

	userPolicy, err := s.policySvc.GetUserReplicationPolicies(ctx, user)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return policy.ReplicationPolicies{}, fmt.Errorf("%w: user replicatoin policy not configured: %v", dom.ErrPolicy, err)
		}
		return policy.ReplicationPolicies{}, err
	}
	for to, priority := range userPolicy.To {
		err = s.policySvc.AddBucketReplicationPolicy(ctx, user, bucket, userPolicy.From, to, priority, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return policy.ReplicationPolicies{}, err
		}
	}

	return userPolicy, nil
}
