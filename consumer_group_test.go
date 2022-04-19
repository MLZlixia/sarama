package sarama

import (
	"context"
	"sync"
	"testing"
)

type handler struct {
	*testing.T
	cancel context.CancelFunc
}

func (h *handler) Setup(s ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(s ConsumerGroupSession) error { return nil }
func (h *handler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		h.Logf("consumed msg %v", msg)
		h.cancel()
		break
	}
	return nil
}

// TestConsumerGroupNewSessionDuringOffsetLoad 如果消费者组临时收到OffsetsLoadInProcess错误响应,
// 消费者组将重试加入和同步操组操作，方法与重新平衡过程相同。
func TestConsumerGroupNewSessionDuringOffsetLoad(t *testing.T) {
	config := NewTestConfig()
	config.ClientID = t.Name()
	config.Version = V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Retry.Max = 2
	config.Consumer.Offsets.AutoCommit.Enable = false

	broker0 := NewMockBroker(t, 0)

	broker0.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my-topic", 0, broker0.BrokerID()),
		"OffsetRequest": NewMockOffsetResponse(t).
			SetOffset("my-topic", 0, OffsetOldest, 0).
			SetOffset("my-topic", 0, OffsetNewest, 1),
		"FindCoordinatorRequest": NewMockFindCoordinatorResponse(t).
			SetCoordinator(CoordinatorGroup, "my-group", broker0),
		"HeartbeatRequest": NewMockHeartbeatResponse(t),
		"JoinGroupRequest": NewMockSequence(
			NewMockJoinGroupResponse(t).SetError(ErrOffsetsLoadInProgress),
			NewMockJoinGroupResponse(t),
		),
		"SyncGroupRequest": NewMockSequence(
			NewMockSyncGroupResponse(t).SetError(ErrOffsetsLoadInProgress),
			NewMockSyncGroupResponse(t).SetMemberAssignment(
				&ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						"my-topic": {0},
					},
				}),
		),
		"OffsetFetchRequest": NewMockOffsetFetchResponse(t).SetOffset(
			"my-group", "my-topic", 0, 0, "", ErrNoError,
		).SetError(ErrNoError),
		"FetchRequest": NewMockSequence(
			NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 0, StringEncoder("foo")).
				SetMessage("my-topic", 0, 1, StringEncoder("bar")),
			NewMockFetchResponse(t, 1),
		),
	})

	group, err := NewConsumerGroup([]string{broker0.Addr()}, "my-group", config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = group.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	h := &handler{t, cancel}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		topics := []string{"my-topic"}
		if err := group.Consume(ctx, topics, h); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	wg.Wait()
}
