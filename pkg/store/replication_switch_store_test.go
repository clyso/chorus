package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestUserReplicationSwitchStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()
	store := NewUserReplicationSwitchStore(c)

	user := "test_user"
	policy := entity.NewUserReplicationPolicy(user, "from_storage", "to_storage")

	// not found
	_, err := store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	_, err = store.Get(ctx, user)
	r.ErrorIs(err, dom.ErrNotFound)
	// no active zero downtime switch
	ok, err := store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.False(ok)

	// add with downtime
	downtimetInvalidCreateStatuses := []entity.ReplicationSwitchStatus{
		// entity.StatusNotStarted, // <- only valid
		entity.StatusInProgress,
		entity.StatusCheckInProgress,
		entity.StatusDone,
		entity.StatusError,
		entity.StatusSkipped,
	}
	dtOpts := entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     true,
		Cron:                strToPtr("*/5 * * * *"),
		StartAt:             timeToPtr(time.Now().UTC()),
		MaxDuration:         time.Hour,
		MaxEventLag:         uint32ToPtr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}

	// cannot create with invalid status
	for _, invalidCreateStatus := range downtimetInvalidCreateStatuses {
		dtSwitch := entity.ReplicationSwitchInfo{
			ReplicationSwitchDowntimeOpts:     dtOpts,
			ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{},
			ReplicationIDStr:                  "",
			LastStatus:                        invalidCreateStatus,
		}
		err = store.Create(ctx, policy, dtSwitch)
		r.Error(err, invalidCreateStatus)
	}
	// create with valid status
	validStatus := entity.StatusNotStarted
	dtSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchDowntimeOpts:     dtOpts,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{},
		ReplicationIDStr:                  "",
		LastStatus:                        validStatus,
	}
	err = store.Create(ctx, policy, dtSwitch)
	r.NoError(err)

	// check that all fields are stored correctly
	got, err := store.GetOp(ctx, user).Get()
	r.NoError(err)
	emptyZeroDtOpts := entity.ReplicationSwitchZeroDowntimeOpts{}
	r.EqualValues(emptyZeroDtOpts, dtSwitch.ReplicationSwitchZeroDowntimeOpts)
	r.EqualValues(dtOpts, got.ReplicationSwitchDowntimeOpts)
	r.Equal(validStatus, got.LastStatus)
	hadID, gotID := entity.UniversalFromUserReplication(policy), got.ReplicationID()
	r.EqualValues(hadID.AsString(), gotID.AsString())
	replID := got.ReplicationID()
	r.NoError(replID.Validate())
	r.NotEmpty(got.ReplicationIDStr)
	r.WithinDuration(time.Now(), got.CreatedAt, time.Second)
	r.Nil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)

	got2, err := store.Get(ctx, user)
	r.NoError(err)
	r.EqualValues(got, got2)

	//check no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.False(ok)

	// create zero downtime switch
	user2 := "test_user2"
	policy2 := entity.NewUserReplicationPolicy(user2, "from_storage", "to_storage")

	// not found
	_, err = store.GetOp(ctx, user2).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user2).Get()
	r.NoError(err)
	r.False(ok)

	// check invalid statuses
	zeroDowntimetInvalidCreateStatuses := []entity.ReplicationSwitchStatus{
		entity.StatusNotStarted,
		// entity.StatusInProgress, // <- only valid
		entity.StatusCheckInProgress,
		entity.StatusDone,
		entity.StatusError,
		entity.StatusSkipped,
	}

	zdOpts := entity.ReplicationSwitchZeroDowntimeOpts{
		MultipartTTL: time.Hour,
	}
	// cannot create with invalid status
	for _, invalidCreateStatus := range zeroDowntimetInvalidCreateStatuses {
		zdSwitch := entity.ReplicationSwitchInfo{
			ReplicationSwitchDowntimeOpts:     entity.ReplicationSwitchDowntimeOpts{},
			ReplicationSwitchZeroDowntimeOpts: zdOpts,
			ReplicationIDStr:                  "",
			LastStatus:                        invalidCreateStatus,
		}
		err = store.Create(ctx, policy2, zdSwitch)
		r.Error(err, invalidCreateStatus)
	}
	// create with valid status
	validStatus = entity.StatusInProgress
	zdSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchDowntimeOpts:     entity.ReplicationSwitchDowntimeOpts{},
		ReplicationSwitchZeroDowntimeOpts: zdOpts,
		ReplicationIDStr:                  "",
		LastStatus:                        validStatus,
	}
	err = store.Create(ctx, policy2, zdSwitch)
	r.NoError(err)

	// check that all fields are stored correctly
	got, err = store.GetOp(ctx, user2).Get()
	r.NoError(err)
	emptyDtOpts := entity.ReplicationSwitchDowntimeOpts{}
	r.EqualValues(emptyDtOpts, zdSwitch.ReplicationSwitchDowntimeOpts)
	r.EqualValues(zdOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.Equal(validStatus, got.LastStatus)
	hadID, gotID = entity.UniversalFromUserReplication(policy2), got.ReplicationID()
	r.EqualValues(hadID.AsString(), gotID.AsString())
	replID = got.ReplicationID()
	r.NoError(replID.Validate())
	r.NotEmpty(got.ReplicationIDStr)
	r.WithinDuration(time.Now(), got.CreatedAt, time.Second)
	r.NotNil(got.LastStartedAt)
	r.WithinDuration(time.Now(), *got.LastStartedAt, time.Second)

	r.Nil(got.DoneAt)
	r.Empty(got.History)

	// get active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user2).Get()
	r.NoError(err)
	r.True(ok)

	// check previous switch not affected
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.EqualValues(dtOpts, got.ReplicationSwitchDowntimeOpts)
	r.EqualValues(emptyZeroDtOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.Equal(entity.StatusNotStarted, got.LastStatus)

	ok, err = store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.False(ok)

	// update first switch downtime opts
	updatedDtOpts := dtOpts
	updatedDtOpts.Cron = strToPtr("*/10 * * * *")
	updatedDtOpts.StartOnInitDone = false
	updatedDtOpts.StartAt = timeToPtr(time.Now().Add(20 * time.Minute).UTC())
	updatedDtOpts.MaxDuration = 2 * time.Hour
	updatedDtOpts.MaxEventLag = nil
	updatedDtOpts.SkipBucketCheck = false
	updatedDtOpts.ContinueReplication = false
	err = store.UpdateDowntimeOpts(ctx, policy, &updatedDtOpts)
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.EqualValues(updatedDtOpts, got.ReplicationSwitchDowntimeOpts)
	r.EqualValues(emptyZeroDtOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.Equal(entity.StatusNotStarted, got.LastStatus)

	// check second switch not affected
	got, err = store.GetOp(ctx, user2).Get()
	r.NoError(err)
	r.EqualValues(zdOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.EqualValues(emptyDtOpts, got.ReplicationSwitchDowntimeOpts)
	r.Equal(entity.StatusInProgress, got.LastStatus)

	// delete first switch
	err = store.DeleteOp(ctx, user).Get()
	r.NoError(err)

	// check deleted
	_, err = store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// check second switch not affected
	got, err = store.GetOp(ctx, user2).Get()
	r.NoError(err)
	r.EqualValues(zdOpts, got.ReplicationSwitchZeroDowntimeOpts)
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user2).Get()
	r.NoError(err)
	r.True(ok)

	// delete second switch
	err = store.DeleteOp(ctx, user2).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, user2).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user2).Get()
	r.NoError(err)
	r.False(ok)
}

func TestUserReplicationSwitchStore_Status(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()
	store := NewUserReplicationSwitchStore(c)

	user := "test_user"
	policy := entity.NewUserReplicationPolicy(user, "from_storage", "to_storage")

	// create with valid status
	dtSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
			StartOnInitDone:     true,
			Cron:                strToPtr("*/5 * * * *"),
			StartAt:             timeToPtr(time.Now().UTC()),
			MaxDuration:         time.Hour,
			MaxEventLag:         uint32ToPtr(100),
			SkipBucketCheck:     true,
			ContinueReplication: true,
		},
		ReplicationIDStr: "",
		LastStatus:       entity.StatusNotStarted,
	}
	err := store.Create(ctx, policy, dtSwitch)
	r.NoError(err)

	// check that all fields are stored correctly
	got, err := store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(entity.StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)
	// check no active zero downtime switch
	ok, err := store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.False(ok)

	// update to in progress
	err = store.UpdateStatusOp(ctx, policy, got.LastStatus, entity.StatusInProgress, "my_message_1").Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(entity.StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Len(got.History, 1)
	r.Contains(got.History[0], "my_message_1")

	// update to done
	err = store.UpdateStatusOp(ctx, policy, got.LastStatus, entity.StatusDone, "my_message_2").Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(entity.StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	// done should be after started
	r.True(got.DoneAt.After(*got.LastStartedAt) || got.DoneAt.Equal(*got.LastStartedAt))
	r.Len(got.History, 2)
	r.Contains(got.History[1], "my_message_2")
	r.Contains(got.History[0], "my_message_1")
	// check no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.False(ok)

	// delete
	err = store.DeleteOp(ctx, user).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// create again with valid status
	err = store.Create(ctx, policy, dtSwitch)
	r.NoError(err)

	// get and check
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(entity.StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)

	// delete
	err = store.DeleteOp(ctx, user).Get()
	r.NoError(err)

	// create zero downtime switch
	zdSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{
			MultipartTTL: time.Hour,
		},
		LastStatus: entity.StatusInProgress,
	}
	err = store.Create(ctx, policy, zdSwitch)
	r.NoError(err)
	// check that all fields are stored correctly
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(entity.StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)
	// check active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.True(ok)

	// update to done
	err = store.UpdateStatusOp(ctx, policy, got.LastStatus, entity.StatusDone, "my_message_3").Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(entity.StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	r.Len(got.History, 1)
	r.Contains(got.History[0], "my_message_3")
	// check no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, user).Get()
	r.NoError(err)
	r.False(ok)
}

func TestBucketReplicationSwitchStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()
	store := NewBucketReplicationSwitchStore(c)

	user, bucket := "test_user", "test_bucket"
	policy := entity.NewBucketRepliationPolicy(user, "from_storage", bucket, "to_storage", bucket)
	policyID := entity.BucketReplicationPolicyID{User: user, FromBucket: bucket}

	// not found
	_, err := store.GetOp(ctx, policyID).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	_, err = store.Get(ctx, policyID)
	r.ErrorIs(err, dom.ErrNotFound)
	// no active zero downtime switch
	ok, err := store.IsZeroDowntimeActiveOp(ctx, policyID).Get()
	r.NoError(err)
	r.False(ok)

	// add with downtime
	downtimetInvalidCreateStatuses := []entity.ReplicationSwitchStatus{
		// entity.StatusNotStarted, // <- only valid
		entity.StatusInProgress,
		entity.StatusCheckInProgress,
		entity.StatusDone,
		entity.StatusError,
		entity.StatusSkipped,
	}
	dtOpts := entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     true,
		Cron:                strToPtr("*/5 * * * *"),
		StartAt:             timeToPtr(time.Now().UTC()),
		MaxDuration:         time.Hour,
		MaxEventLag:         uint32ToPtr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}

	// cannot create with invalid status
	for _, invalidCreateStatus := range downtimetInvalidCreateStatuses {
		dtSwitch := entity.ReplicationSwitchInfo{
			ReplicationSwitchDowntimeOpts:     dtOpts,
			ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{},
			ReplicationIDStr:                  "",
			LastStatus:                        invalidCreateStatus,
		}
		err = store.Create(ctx, policy, dtSwitch)
		r.Error(err, invalidCreateStatus)
	}
	// create with valid status
	validStatus := entity.StatusNotStarted
	dtSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchDowntimeOpts:     dtOpts,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{},
		ReplicationIDStr:                  "",
		LastStatus:                        validStatus,
	}
	err = store.Create(ctx, policy, dtSwitch)
	r.NoError(err)

	// check that all fields are stored correctly
	got, err := store.GetOp(ctx, policyID).Get()
	r.NoError(err)
	emptyZeroDtOpts := entity.ReplicationSwitchZeroDowntimeOpts{}
	r.EqualValues(emptyZeroDtOpts, dtSwitch.ReplicationSwitchZeroDowntimeOpts)
	r.EqualValues(dtOpts, got.ReplicationSwitchDowntimeOpts)
	r.Equal(validStatus, got.LastStatus)
	hadID, gotID := entity.UniversalFromBucketReplication(policy), got.ReplicationID()
	r.EqualValues(hadID.AsString(), gotID.AsString())
	r.NoError(gotID.Validate())
	r.NotEmpty(got.ReplicationIDStr)
	r.WithinDuration(time.Now(), got.CreatedAt, time.Second)
	r.Nil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)

	got2, err := store.Get(ctx, policyID)
	r.NoError(err)
	r.EqualValues(got, got2)

	//check no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policyID).Get()
	r.NoError(err)
	r.False(ok)

	// create zero downtime switch
	bucket2 := "test_bucket2"
	policy2 := entity.NewBucketRepliationPolicy(user, "from_storage", bucket2, "to_storage", bucket2)
	policyID2 := entity.BucketReplicationPolicyID{User: user, FromBucket: bucket2}

	// not found
	_, err = store.GetOp(ctx, policyID2).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policyID2).Get()
	r.NoError(err)
	r.False(ok)

	// check invalid statuses
	zeroDowntimetInvalidCreateStatuses := []entity.ReplicationSwitchStatus{
		entity.StatusNotStarted,
		// entity.StatusInProgress, // <- only valid
		entity.StatusCheckInProgress,
		entity.StatusDone,
		entity.StatusError,
		entity.StatusSkipped,
	}

	zdOpts := entity.ReplicationSwitchZeroDowntimeOpts{
		MultipartTTL: time.Hour,
	}
	// cannot create with invalid status
	for _, invalidCreateStatus := range zeroDowntimetInvalidCreateStatuses {
		zdSwitch := entity.ReplicationSwitchInfo{
			ReplicationSwitchDowntimeOpts:     entity.ReplicationSwitchDowntimeOpts{},
			ReplicationSwitchZeroDowntimeOpts: zdOpts,
			ReplicationIDStr:                  "",
			LastStatus:                        invalidCreateStatus,
		}
		err = store.Create(ctx, policy2, zdSwitch)
		r.Error(err, invalidCreateStatus)
	}
	// create with valid status
	validStatus = entity.StatusInProgress
	zdSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchDowntimeOpts:     entity.ReplicationSwitchDowntimeOpts{},
		ReplicationSwitchZeroDowntimeOpts: zdOpts,
		ReplicationIDStr:                  "",
		LastStatus:                        validStatus,
	}
	err = store.Create(ctx, policy2, zdSwitch)
	r.NoError(err)

	// check that all fields are stored correctly
	got, err = store.GetOp(ctx, policyID2).Get()
	r.NoError(err)
	emptyDtOpts := entity.ReplicationSwitchDowntimeOpts{}
	r.EqualValues(emptyDtOpts, zdSwitch.ReplicationSwitchDowntimeOpts)
	r.EqualValues(zdOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.Equal(validStatus, got.LastStatus)
	hadID, gotID = entity.UniversalFromBucketReplication(policy2), got.ReplicationID()
	r.EqualValues(hadID.AsString(), gotID.AsString())
	r.NoError(gotID.Validate())
	r.NotEmpty(got.ReplicationIDStr)
	r.WithinDuration(time.Now(), got.CreatedAt, time.Second)
	r.NotNil(got.LastStartedAt)
	r.WithinDuration(time.Now(), *got.LastStartedAt, time.Second)
	r.Nil(got.DoneAt)
	r.Empty(got.History)

	// get active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policyID2).Get()
	r.NoError(err)
	r.True(ok)

	// check previous switch not affected
	got, err = store.GetOp(ctx, policyID).Get()
	r.NoError(err)
	r.EqualValues(dtOpts, got.ReplicationSwitchDowntimeOpts)
	r.EqualValues(emptyZeroDtOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.Equal(entity.StatusNotStarted, got.LastStatus)

	ok, err = store.IsZeroDowntimeActiveOp(ctx, policyID).Get()
	r.NoError(err)
	r.False(ok)

	// update first switch downtime opts
	updatedDtOpts := dtOpts
	updatedDtOpts.Cron = strToPtr("*/10 * * * *")
	updatedDtOpts.StartOnInitDone = false
	updatedDtOpts.StartAt = timeToPtr(time.Now().Add(20 * time.Minute).UTC())
	updatedDtOpts.MaxDuration = 2 * time.Hour
	updatedDtOpts.MaxEventLag = nil
	updatedDtOpts.SkipBucketCheck = false
	updatedDtOpts.ContinueReplication = false
	err = store.UpdateDowntimeOpts(ctx, policy, &updatedDtOpts)
	r.NoError(err)
	got, err = store.GetOp(ctx, policyID).Get()
	r.NoError(err)
	r.EqualValues(updatedDtOpts, got.ReplicationSwitchDowntimeOpts)
	r.EqualValues(emptyZeroDtOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.Equal(entity.StatusNotStarted, got.LastStatus)

	// check second switch not affected
	got, err = store.GetOp(ctx, policyID2).Get()
	r.NoError(err)
	r.EqualValues(zdOpts, got.ReplicationSwitchZeroDowntimeOpts)
	r.EqualValues(emptyDtOpts, got.ReplicationSwitchDowntimeOpts)
	r.Equal(entity.StatusInProgress, got.LastStatus)

	// delete first switch
	err = store.DeleteOp(ctx, policyID).Get()
	r.NoError(err)

	// check deleted
	_, err = store.GetOp(ctx, policyID).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// check second switch not affected
	got, err = store.GetOp(ctx, policyID2).Get()
	r.NoError(err)
	r.EqualValues(zdOpts, got.ReplicationSwitchZeroDowntimeOpts)
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policyID2).Get()
	r.NoError(err)
	r.True(ok)

	// delete second switch
	err = store.DeleteOp(ctx, policyID2).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policyID2).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policyID2).Get()
	r.NoError(err)
	r.False(ok)
}

func strToPtr(s string) *string {
	return &s
}

func uint32ToPtr(u uint32) *uint32 {
	return &u
}

func timeToPtr(t time.Time) *time.Time {
	return &t
}

func TestBucketReplicationSwitchStore_Status(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()
	store := NewBucketReplicationSwitchStore(c)

	user, bucket := "test_user", "test_bucket"
	policy := entity.NewBucketRepliationPolicy(user, "from_storage", bucket, "to_storage", bucket)

	// create with valid status
	dtSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
			StartOnInitDone:     true,
			Cron:                strToPtr("*/5 * * * *"),
			StartAt:             timeToPtr(time.Now().UTC()),
			MaxDuration:         time.Hour,
			MaxEventLag:         uint32ToPtr(100),
			SkipBucketCheck:     true,
			ContinueReplication: true,
		},
		ReplicationIDStr: "",
		LastStatus:       entity.StatusNotStarted,
	}
	err := store.Create(ctx, policy, dtSwitch)
	r.NoError(err)

	// check that all fields are stored correctly
	got, err := store.GetOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.Equal(entity.StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)
	// check no active zero downtime switch
	ok, err := store.IsZeroDowntimeActiveOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.False(ok)

	// update to in progress
	err = store.UpdateStatusOp(ctx, policy, got.LastStatus, entity.StatusInProgress, "my_message_1").Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.Equal(entity.StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Len(got.History, 1)
	r.Contains(got.History[0], "my_message_1")

	// update to done
	err = store.UpdateStatusOp(ctx, policy, got.LastStatus, entity.StatusDone, "my_message_2").Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.Equal(entity.StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	// done should be after started
	r.True(got.DoneAt.After(*got.LastStartedAt) || got.DoneAt.Equal(*got.LastStartedAt))
	r.Len(got.History, 2)
	r.Contains(got.History[1], "my_message_2")
	r.Contains(got.History[0], "my_message_1")
	// check no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.False(ok)

	// delete
	err = store.DeleteOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy.LookupID()).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// create again with valid status
	err = store.Create(ctx, policy, dtSwitch)
	r.NoError(err)

	// get and check
	got, err = store.GetOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.Equal(entity.StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)

	// delete
	err = store.DeleteOp(ctx, policy.LookupID()).Get()
	r.NoError(err)

	// create zero downtime switch
	zdSwitch := entity.ReplicationSwitchInfo{
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{
			MultipartTTL: time.Hour,
		},
		LastStatus: entity.StatusInProgress,
	}
	err = store.Create(ctx, policy, zdSwitch)
	r.NoError(err)
	// check that all fields are stored correctly
	got, err = store.GetOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.Equal(entity.StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.Empty(got.History)
	// check active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.True(ok)

	// update to done
	err = store.UpdateStatusOp(ctx, policy, got.LastStatus, entity.StatusDone, "my_message_3").Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.Equal(entity.StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	r.Len(got.History, 1)
	r.Contains(got.History[0], "my_message_3")
	// check no active zero downtime switch
	ok, err = store.IsZeroDowntimeActiveOp(ctx, policy.LookupID()).Get()
	r.NoError(err)
	r.False(ok)
}

func TestReplicationSwitchStore_StatusUpdate(t *testing.T) {
	c := testutil.SetupRedis(t)
	tests := []struct {
		name     string
		existing entity.ReplicationSwitchInfo
		opts     *entity.ReplicationSwitchDowntimeOpts
		wantErr  bool
	}{
		{
			name: "set switch with optional values",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusNotStarted,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     (10 * time.Second),
				MaxEventLag:     nil, // Should trigger HDEL
			},
			wantErr: false,
		},
		{
			name: "set switch with all values",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusNotStarted,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now().UTC()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			wantErr: false,
		},
		{
			name: "update existing data with non-empty opts",
			existing: entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					StartOnInitDone:     true,
					Cron:                stringPtr("0 0 * * *"),
					StartAt:             timePtr(time.Now().UTC()),
					MaxDuration:         (10 * time.Second),
					MaxEventLag:         uint32Ptr(100),
					SkipBucketCheck:     true,
					ContinueReplication: true,
				},
				LastStatus: entity.StatusNotStarted,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     false,
				Cron:                stringPtr("1 1 * * *"),
				StartAt:             timePtr(time.Now().Add(1 * time.Hour).UTC()),
				MaxDuration:         (18 * time.Second),
				MaxEventLag:         uint32Ptr(123),
				SkipBucketCheck:     false,
				ContinueReplication: false,
			},
			wantErr: false,
		},
		{
			name: "update existing data with empty opts",
			existing: entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					StartOnInitDone:     true,
					Cron:                stringPtr("0 0 * * *"),
					StartAt:             timePtr(time.Now().UTC()),
					MaxDuration:         (10 * time.Second),
					MaxEventLag:         uint32Ptr(100),
					SkipBucketCheck:     true,
					ContinueReplication: true,
				},
				LastStatus: entity.StatusNotStarted,
			},
			opts:    &entity.ReplicationSwitchDowntimeOpts{}, // All pointers nil, bools false
			wantErr: false,
		},
		{
			name: "delete existing data with nil opts",
			existing: entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					StartOnInitDone:     true,
					Cron:                stringPtr("0 0 * * *"),
					StartAt:             timePtr(time.Now().UTC()),
					MaxDuration:         (10 * time.Second),
					MaxEventLag:         uint32Ptr(100),
					SkipBucketCheck:     true,
					ContinueReplication: true,
				},
				LastStatus: entity.StatusNotStarted,
			},
			opts:    nil,
			wantErr: false,
		},
		{
			name: "update existing data with new values",
			existing: entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					StartOnInitDone: true,
					Cron:            stringPtr("0 0 * * *"),
					MaxDuration:     (10 * time.Second),
					MaxEventLag:     uint32Ptr(100),
				},
				LastStatus: entity.StatusNotStarted,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: false,
				Cron:            stringPtr("1 1 * * *"),
				MaxDuration:     (20 * time.Second),
				MaxEventLag:     nil, // Should delete this field
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run("user: "+tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatalf("failed to flush Redis: %v", err)
			}
			ctx := t.Context()
			r := require.New(t)
			store := NewUserReplicationSwitchStore(c)
			policy := entity.UserReplicationPolicy{
				User:        "u",
				FromStorage: "f",
				ToStorage:   "t",
			}
			err := store.Create(ctx, policy, tt.existing)
			r.NoError(err)
			err = store.UpdateDowntimeOpts(ctx, policy, tt.opts)
			if tt.wantErr {
				r.Error(err)
				return
			} else {
				r.NoError(err)
			}
			got, err := store.GetOp(ctx, policy.User).Get()
			r.NoError(err)

			if tt.opts == nil {
				// All fields should be reset to zero values
				r.Equal(entity.ReplicationSwitchDowntimeOpts{}, got.ReplicationSwitchDowntimeOpts)
				return
			}
			r.Equal(tt.opts.StartOnInitDone, got.ReplicationSwitchDowntimeOpts.StartOnInitDone)
			r.Equal(tt.opts.Cron, got.ReplicationSwitchDowntimeOpts.Cron)
			r.Equal(tt.opts.StartAt, got.ReplicationSwitchDowntimeOpts.StartAt)
			r.Equal(tt.opts.MaxDuration, got.ReplicationSwitchDowntimeOpts.MaxDuration)
			r.Equal(tt.opts.MaxEventLag, got.ReplicationSwitchDowntimeOpts.MaxEventLag)
			r.Equal(tt.opts.SkipBucketCheck, got.ReplicationSwitchDowntimeOpts.SkipBucketCheck)
			r.Equal(tt.opts.ContinueReplication, got.ReplicationSwitchDowntimeOpts.ContinueReplication)

		})
		t.Run("bucket: "+tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatalf("failed to flush Redis: %v", err)
			}
			ctx := t.Context()
			r := require.New(t)
			store := NewBucketReplicationSwitchStore(c)
			policy := entity.BucketReplicationPolicy{
				User:        "u",
				FromStorage: "f",
				ToStorage:   "t",
				FromBucket:  "b",
				ToBucket:    "b",
			}
			err := store.Create(ctx, policy, tt.existing)
			r.NoError(err)
			err = store.UpdateDowntimeOpts(ctx, policy, tt.opts)
			if tt.wantErr {
				r.Error(err)
				return
			} else {
				r.NoError(err)
			}
			got, err := store.GetOp(ctx, policy.LookupID()).Get()
			r.NoError(err)

			if tt.opts == nil {
				// All fields should be reset to zero values
				r.Equal(entity.ReplicationSwitchDowntimeOpts{}, got.ReplicationSwitchDowntimeOpts)
				return
			}
			r.Equal(tt.opts.StartOnInitDone, got.ReplicationSwitchDowntimeOpts.StartOnInitDone)
			r.Equal(tt.opts.Cron, got.ReplicationSwitchDowntimeOpts.Cron)
			r.Equal(tt.opts.StartAt, got.ReplicationSwitchDowntimeOpts.StartAt)
			r.Equal(tt.opts.MaxDuration, got.ReplicationSwitchDowntimeOpts.MaxDuration)
			r.Equal(tt.opts.MaxEventLag, got.ReplicationSwitchDowntimeOpts.MaxEventLag)
			r.Equal(tt.opts.SkipBucketCheck, got.ReplicationSwitchDowntimeOpts.SkipBucketCheck)
			r.Equal(tt.opts.ContinueReplication, got.ReplicationSwitchDowntimeOpts.ContinueReplication)

		})
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}
