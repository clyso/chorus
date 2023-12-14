package policy_helper

import (
	"context"
	"errors"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"golang.org/x/sync/errgroup"
)

func CreateMainFollowerPolicies(
	ctx context.Context,
	conf s3.StorageConfig,
	clients s3client.Service,
	policySvc policy.Service,
	taskClient *asynq.Client) error {

	g, ctx := errgroup.WithContext(ctx)

	for u := range conf.Storages[conf.Main()].Credentials {
		user := u
		if conf.CreateRouting {
			g.Go(func() error {
				return createRouting(ctx, policySvc, user, conf.Main())
			})
		}
		if conf.CreateReplication {
			for _, to := range conf.Followers() {
				toCopy := to
				g.Go(func() error {
					return createReplication(ctx, clients, policySvc, taskClient, user, conf.Main(), toCopy)
				})
			}
		}
	}
	return g.Wait()
}

func createRouting(
	ctx context.Context,
	policySvc policy.Service,
	user, main string) error {

	err := policySvc.AddUserRoutingPolicy(ctx, user, main)
	if err != nil {
		if errors.Is(err, dom.ErrAlreadyExists) {
			return nil
		}
		return err
	}

	return nil
}

func createReplication(
	ctx context.Context,
	clients s3client.Service,
	policySvc policy.Service,
	taskClient *asynq.Client,
	user, from, to string) error {
	_, err := policySvc.GetUserReplicationPolicies(ctx, user)
	if err == nil {
		// already exists
		return nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return err
	}
	err = policySvc.AddUserReplicationPolicy(ctx, user, from, to, tasks.PriorityDefault1)
	if err != nil {
		if errors.Is(err, dom.ErrAlreadyExists) {
			return nil
		}
		return err
	}
	ctx = xctx.SetUser(ctx, user)
	client, err := clients.GetByName(ctx, from)
	if err != nil {
		return err
	}
	buckets, err := client.S3().ListBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		err = policySvc.AddBucketReplicationPolicy(ctx, user, bucket.Name, from, to, tasks.PriorityDefault1, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return err
		}
		task, err := tasks.NewTask(ctx, tasks.BucketCreatePayload{
			Sync: tasks.Sync{
				FromStorage: from,
				ToStorage:   to,
			},
			Bucket: bucket.Name,
		})
		if err != nil {
			return err
		}
		_, err = taskClient.EnqueueContext(ctx, task)
		if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
			return err
		}
	}
	return nil
}
