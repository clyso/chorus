/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notifications

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/minio/minio-go/v7/pkg/notification"

	"slices"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3client"
)

type Service struct {
	clients s3client.Service
}

func NewService(clients s3client.Service) *Service {
	return &Service{clients: clients}
}

func (s *Service) SubscribeToBucketNotifications(ctx context.Context, storage, user, bucket, agentURL string) error {
	client, err := s.clients.GetByName(ctx, user, storage)
	if err != nil {
		return err
	}
	name := topicName(user)
	arn, err := s.getOrCreateTopic(ctx, name, client.SNS(), agentURL)
	if err != nil {
		return err
	}
	notifications, err := client.S3().GetBucketNotification(ctx, bucket)
	if err != nil {
		return err
	}
	id := notificationID(user, bucket)
	found := -1
	for i, topic := range notifications.TopicConfigs {
		if topic.ID == id {
			found = i
			break
		}
	}

	config := notification.TopicConfig{
		Config: notification.Config{
			ID:     id,
			Events: []notification.EventType{notification.ObjectCreatedAll, notification.ObjectRemovedAll},
		},
		Topic: arn,
	}

	if found != -1 {
		notifications.TopicConfigs[found] = config
	} else {
		notifications.TopicConfigs = append(notifications.TopicConfigs, config)
	}
	return client.S3().SetBucketNotification(ctx, bucket, notifications)
}

func (s *Service) DeleteBucketNotification(ctx context.Context, storage, user, bucket string) error {
	client, err := s.clients.GetByName(ctx, user, storage)
	if err != nil {
		return err
	}
	notifications, err := client.S3().GetBucketNotification(ctx, bucket)
	if err != nil {
		return err
	}
	id := notificationID(user, bucket)
	toRemove := -1
	for i, topic := range notifications.TopicConfigs {
		if topic.ID == id {
			toRemove = i
			break
		}
	}
	if toRemove == -1 {
		return nil
	}
	notifications.TopicConfigs = slices.Delete(notifications.TopicConfigs, toRemove, toRemove+1)
	return client.S3().SetBucketNotification(ctx, bucket, notifications)
}

func topicName(user string) string {
	return fmt.Sprintf("chorus-%s", user)
}

func notificationID(user, bucket string) string {
	bucket = strings.ReplaceAll(bucket, "-", "--")
	user = strings.ReplaceAll(user, "-", "--")
	return fmt.Sprintf("chorus-%s-%s", user, bucket)
}

func UserIDFromNotificationID(id string) (string, error) {
	if !strings.HasPrefix(id, "chorus-") {
		return "", fmt.Errorf("%w: notification id should start from 'chorus-': %s", dom.ErrInternal, id)
	}
	id = strings.TrimPrefix(id, "chorus-")
	if len(id) < 5 {
		return "", fmt.Errorf("%w: unable to extract user from notification id: %s", dom.ErrInternal, id)
	}

	for i := 1; i < len(id)-1; i++ {
		if id[i] == '-' && id[i-1] != '-' && id[i+1] != '-' {
			return strings.ReplaceAll(id[:i], "--", "-"), nil
		}
	}
	return "", fmt.Errorf("%w: unable to extract user from notification id: %s", dom.ErrInternal, id)
}

func (s *Service) getOrCreateTopic(ctx context.Context, name string, client *sns.Client, agentURL string) (string, error) {
	arn, err := s.findTopicARNByName(ctx, name, client, nil)
	if err == nil {
		return arn, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return "", err
	}
	// not found - create topic
	attributes := map[string]string{
		"persistent":    "true",
		"push-endpoint": agentURL,
	}
	topic, err := client.CreateTopic(context.Background(), &sns.CreateTopicInput{
		Name:       aws.String(name),
		Attributes: attributes,
	})
	if err != nil {
		return "", err
	}
	if topic.TopicArn == nil {
		return "", fmt.Errorf("%w: topic arn is nil", dom.ErrInternal)
	}
	return *topic.TopicArn, nil
}

func (s *Service) findTopicARNByName(ctx context.Context, name string, client *sns.Client, nextToken *string) (string, error) {
	topics, err := client.ListTopics(ctx, &sns.ListTopicsInput{NextToken: nextToken})
	if err != nil {
		return "", err
	}
	if len(topics.Topics) == 0 {
		return "", dom.ErrNotFound
	}
	for _, topic := range topics.Topics {
		if topic.TopicArn != nil && strings.HasSuffix(*topic.TopicArn, name) {
			return *topic.TopicArn, nil
		}
	}
	if topics.NextToken != nil && *topics.NextToken != "" {
		return s.findTopicARNByName(ctx, name, client, topics.NextToken)
	}
	return "", dom.ErrNotFound
}
