package sarama

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a consumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

// ConsumerGroup is responsible for dividing up processing of topics and partitions
// over a collection of processes (the members of the consumer group).
type ConsumerGroup interface {
	// Consumer 加入给定主题列表的消费者集群, 并通过ConsumerGroupHandler启动阻塞ConsumerGroupSession
	// session 的生命周期由一下步骤决定
	// 1 消费者加入集群，触发分区再分配
	// 2 处理开始前调用handler的Setup的hook来通知调用者，并运行调用者对状态做有必要的修改。
	// 3 对于每个分配的claims,处理程序的consumerClaim()函数将在一个单独的goroutine中调用，该goroutine要求它是线程安全的。必须小心保护任何状态，防止并发读/写。
	// 4 session将持续，直到其中一个consumerClaim()函数退出。这可以是在取消父上下文时，也可以是在启动服务器端重新平衡循环时。
	// 5 一旦所有consumerClaim()循环退出，就会调用处理程序的Cleanup()钩子，以允许用户在重新平衡之前做某些必要操作(比如提交offset)。
	// 6 在claims释放之前，最后一次提交offset标记
	//
	// 一旦触发再平衡session必须在Config.Consumer.Group.Rebalance.Timeout时间内完成，
	// 这意味着ConsumerClaim()必须尽快退出，以便有时间进行Cleanup()和最终偏移量的提交。
	// 如果超过超时时间，kafka会将consumer从组中移除，从而提交失败。
	// 应该在无限循环中调用此方法，当服务器端重新平衡时，需要重新创建者consumer的session以获得新的claims。
	Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error

	// Errors returns a read channel of errors that occurred during the consumer life-cycle.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error

	// 暂停对指定分区的请求。在使用Resume()/ResumeAll()恢复这些分区之前，broker不会从这些分区返回任何数据。
	// 此方法不影响分区订阅，当使用自动分配时，不会导致组重新平衡。
	Pause(partitions map[string][]int32)

	// Resume 恢复使用Pause（）/PauseAll（）暂停的指定分区。
	// broker将返回这些分区的数据
	Resume(partitions map[string][]int32)

	// Pause suspends fetching from all partitions. Future calls to the broker will not return any
	// records from these partitions until they have been resumed using Resume()/ResumeAll().
	// Note that this method does not affect partition subscription.
	// In particular, it does not cause a group rebalance when automatic assignment is used.
	PauseAll()

	// Resume resumes all partitions which have been paused with Pause()/PauseAll().
	// New calls to the broker will return records from these partitions if there are any to be fetched.
	ResumeAll()
}

type consumerGroup struct {
	client Client

	config   *Config
	consumer Consumer
	groupID  string
	memberID string
	errors   chan error

	lock      sync.Mutex
	closed    chan none
	closeOnce sync.Once

	userData []byte
}

// NewConsumerGroup creates a new consumer group the given broker addresses and configuration.
func NewConsumerGroup(addrs []string, groupID string, config *Config) (ConsumerGroup, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c, err := newConsumerGroup(groupID, client)
	if err != nil {
		_ = client.Close()
	}
	return c, err
}

// NewConsumerGroupFromClient creates a new consumer group using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
// PLEASE NOTE: consumer groups can only re-use but not share clients.
func NewConsumerGroupFromClient(groupID string, client Client) (ConsumerGroup, error) {
	// For clients passed in by the client, ensure we don't
	// call Close() on it.
	cli := &nopCloserClient{client}
	return newConsumerGroup(groupID, cli)
}

func newConsumerGroup(groupID string, client Client) (ConsumerGroup, error) {
	config := client.Config()
	if !config.Version.IsAtLeast(V0_10_2_0) {
		return nil, ConfigurationError("consumer groups require Version to be >= V0_10_2_0")
	}

	consumer, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &consumerGroup{
		client:   client,
		consumer: consumer,
		config:   config,
		groupID:  groupID,
		errors:   make(chan error, config.ChannelBufferSize),
		closed:   make(chan none),
		userData: config.Consumer.Group.Member.UserData,
	}, nil
}

// Errors implements ConsumerGroup.
func (c *consumerGroup) Errors() <-chan error { return c.errors }

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		// leave group
		if e := c.leave(); e != nil {
			err = e
		}

		// drain errors
		go func() {
			close(c.errors)
		}()
		for e := range c.errors {
			err = e
		}

		if e := c.client.Close(); e != nil {
			err = e
		}
	})
	return
}

// Consume implements ConsumerGroup.
func (c *consumerGroup) Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error {
	// Ensure group is not closed
	select {
	case <-c.closed:
		return ErrClosedConsumerGroup
	default:
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	// 刷新指定topic的集群的元数据
	if err := c.client.RefreshMetadata(topics...); err != nil {
		return err
	}

	// Init session
	sess, err := c.newSession(ctx, topics, handler, c.config.Consumer.Group.Rebalance.Retry.Max)
	if errors.Is(err, ErrClosedClient) {
		return ErrClosedConsumerGroup
	} else if err != nil {
		return err
	}

	/*
		当任何主题分区号发生更改时，loop check topic partition number changed（循环检查主题分区号更改）将触发重新平衡避免再次调用函数，
		该函数将生成超过LoopCheckPartitionNumber（循环检查主题分区号）的协程
	*/
	go c.loopCheckPartitionNumbers(topics, sess)

	// 等待结束的信号
	<-sess.ctx.Done()

	// session释放
	return sess.release(true)
}

// Pause implements ConsumerGroup.
func (c *consumerGroup) Pause(partitions map[string][]int32) {
	c.consumer.Pause(partitions)
}

// Resume implements ConsumerGroup.
func (c *consumerGroup) Resume(partitions map[string][]int32) {
	c.consumer.Resume(partitions)
}

// PauseAll implements ConsumerGroup.
func (c *consumerGroup) PauseAll() {
	c.consumer.PauseAll()
}

// ResumeAll implements ConsumerGroup.
func (c *consumerGroup) ResumeAll() {
	c.consumer.ResumeAll()
}

func (c *consumerGroup) retryNewSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int, refreshCoordinator bool) (*consumerGroupSession, error) {
	select {
	case <-c.closed:
		return nil, ErrClosedConsumerGroup
	case <-time.After(c.config.Consumer.Group.Rebalance.Retry.Backoff):
	}

	if refreshCoordinator {
		err := c.client.RefreshCoordinator(c.groupID)
		if err != nil {
			return c.retryNewSession(ctx, topics, handler, retries, true)
		}
	}

	return c.newSession(ctx, topics, handler, retries-1)
}

func (c *consumerGroup) newSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int) (*consumerGroupSession, error) {
	// 获群组协调器(不同群组可以有不同的协调器)
	// consumer向协调器发送心跳来维持他们和群组的从属关系，以及对分区的所有权关系
	// 对应于本client，会开启异步心跳检测，不会阻塞消息的轮序
	// 正常时间内心跳=>consumer活跃
	// 过长时间心跳=> consumer的session过期=>consumer死亡=>触发再平衡

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		if retries <= 0 {
			return nil, err
		}

		return c.retryNewSession(ctx, topics, handler, retries, true)
	}

	var (
		metricRegistry          = c.config.MetricRegistry
		consumerGroupJoinTotal  metrics.Counter
		consumerGroupJoinFailed metrics.Counter
		consumerGroupSyncTotal  metrics.Counter
		consumerGroupSyncFailed metrics.Counter
	)

	if metricRegistry != nil {
		consumerGroupJoinTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-total-%s", c.groupID), metricRegistry)
		consumerGroupJoinFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-failed-%s", c.groupID), metricRegistry)
		consumerGroupSyncTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-total-%s", c.groupID), metricRegistry)
		consumerGroupSyncFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-failed-%s", c.groupID), metricRegistry)
	}

	// Join consumer group
	// 加入消费者群组，向群组协调器发送一个joinGroup请求。
	// 第一个加入加入群组的消费者是"群主"
	join, err := c.joinGroupRequest(coordinator, topics)
	if consumerGroupJoinTotal != nil {
		// 加入消费者群组的总数+1
		consumerGroupJoinTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(join.Err, ErrNoError) {
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
	}
	switch join.Err {
	case ErrNoError:
		c.memberID = join.MemberId
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		if retries <= 0 {
			return nil, join.Err
		}
		return c.retryNewSession(ctx, topics, handler, retries, true)
	default:
		return nil, join.Err
	}

	// 如果是以leader的身份加入的，触发给每个分组成员重新分配分区
	var plan BalanceStrategyPlan
	if join.LeaderId == join.MemberId {
		// 以leader身份加入，也就是群主
		// 获取所有活跃的成员, 根据元数据解析的本地缓存
		members, err := join.GetMembers()
		if err != nil {
			return nil, err
		}

		// 对分区和成员的所有权，按照在平衡策略生成新的分配计划
		plan, err = c.balance(members)
		if err != nil {
			return nil, err
		}
	}

	// Sync consumer group
	// 把在平衡计划发送到分组协调器，触发再一次平衡
	groupRequest, err := c.syncGroupRequest(coordinator, plan, join.GenerationId)
	if consumerGroupSyncTotal != nil {
		consumerGroupSyncTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(groupRequest.Err, ErrNoError) {
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
	}

	switch groupRequest.Err {
	case ErrNoError:
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		if retries <= 0 {
			return nil, groupRequest.Err
		}
		return c.retryNewSession(ctx, topics, handler, retries, true)
	default:
		return nil, groupRequest.Err
	}

	// Retrieve and sort claims
	var claims map[string][]int32
	if len(groupRequest.MemberAssignment) > 0 {
		// 获取再平衡之后分区和consumer的分配关系
		members, err := groupRequest.GetMemberAssignment()
		if err != nil {
			return nil, err
		}
		claims = members.Topics // 主题和分区的对应关系

		// 在有状态平衡策略的情况下，保留返回的分配元数据，否则，重置静态定义的conusmer组元数据
		if members.UserData != nil {
			c.userData = members.UserData
		} else {
			c.userData = c.config.Consumer.Group.Member.UserData
		}

		for _, partitions := range claims {
			// 对分区进行排序
			sort.Sort(int32Slice(partitions))
		}
	}

	// 创建一个新的consumer会话，memberid，加入的新的consumer
	return newConsumerGroupSession(ctx, c, claims, join.MemberId, join.GenerationId, handler)
}

func (c *consumerGroup) joinGroupRequest(coordinator *Broker, topics []string) (*JoinGroupResponse, error) {
	req := &JoinGroupRequest{
		GroupId:        c.groupID,
		MemberId:       c.memberID,
		SessionTimeout: int32(c.config.Consumer.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}
	if c.config.Version.IsAtLeast(V0_10_1_0) {
		req.Version = 1
		req.RebalanceTimeout = int32(c.config.Consumer.Group.Rebalance.Timeout / time.Millisecond)
	}

	meta := &ConsumerGroupMemberMetadata{
		Topics:   topics,
		UserData: c.userData,
	}
	strategy := c.config.Consumer.Group.Rebalance.Strategy
	if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
		return nil, err
	}

	return coordinator.JoinGroup(req)
}

func (c *consumerGroup) syncGroupRequest(coordinator *Broker, plan BalanceStrategyPlan, generationID int32) (*SyncGroupResponse, error) {
	req := &SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: generationID,
	}
	strategy := c.config.Consumer.Group.Rebalance.Strategy
	for memberID, topics := range plan {
		assignment := &ConsumerGroupMemberAssignment{Topics: topics}
		userDataBytes, err := strategy.AssignmentData(memberID, topics, generationID)
		if err != nil {
			return nil, err
		}
		assignment.UserData = userDataBytes
		if err := req.AddGroupAssignmentMember(memberID, assignment); err != nil {
			return nil, err
		}
	}
	return coordinator.SyncGroup(req)
}

func (c *consumerGroup) heartbeatRequest(coordinator *Broker, memberID string, generationID int32) (*HeartbeatResponse, error) {
	req := &HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	}

	return coordinator.Heartbeat(req)
}

func (c *consumerGroup) balance(members map[string]ConsumerGroupMemberMetadata) (BalanceStrategyPlan, error) {
	topics := make(map[string][]int32)
	for _, meta := range members {
		for _, topic := range meta.Topics {
			topics[topic] = nil
		}
	}

	for topic := range topics {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		topics[topic] = partitions
	}

	strategy := c.config.Consumer.Group.Rebalance.Strategy
	return strategy.Plan(members, topics)
}

// Leaves the cluster, called by Close.
func (c *consumerGroup) leave() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.memberID == "" {
		return nil
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	resp, err := coordinator.LeaveGroup(&LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	})
	if err != nil {
		_ = coordinator.Close()
		return err
	}

	// Unset memberID
	c.memberID = ""

	// Check response
	switch resp.Err {
	case ErrRebalanceInProgress, ErrUnknownMemberId, ErrNoError:
		return nil
	default:
		return resp.Err
	}
}

func (c *consumerGroup) handleError(err error, topic string, partition int32) {
	var consumerError *ConsumerError
	if ok := errors.As(err, &consumerError); !ok && topic != "" && partition > -1 {
		err = &ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
	}

	if !c.config.Consumer.Return.Errors {
		Logger.Println(err)
		return
	}

	select {
	case <-c.closed:
		// consumer is closed
		return
	default:
	}

	select {
	case c.errors <- err:
	default:
		// no error listener
	}
}

func (c *consumerGroup) loopCheckPartitionNumbers(topics []string, session *consumerGroupSession) {
	pause := time.NewTicker(c.config.Metadata.RefreshFrequency)
	defer session.cancel()
	defer pause.Stop()
	var oldTopicToPartitionNum map[string]int
	var err error
	if oldTopicToPartitionNum, err = c.topicToPartitionNumbers(topics); err != nil {
		return
	}
	for {
		if newTopicToPartitionNum, err := c.topicToPartitionNumbers(topics); err != nil {
			return
		} else {
			for topic, num := range oldTopicToPartitionNum {
				if newTopicToPartitionNum[topic] != num {
					return // trigger the end of the session on exit
				}
			}
		}
		select {
		case <-pause.C:
		case <-session.ctx.Done():
			Logger.Printf(
				"consumergroup/%s loop check partition number coroutine will exit, topics %s\n",
				c.groupID, topics)
			// if session closed by other, should be exited
			return
		case <-c.closed:
			return
		}
	}
}

func (c *consumerGroup) topicToPartitionNumbers(topics []string) (map[string]int, error) {
	topicToPartitionNum := make(map[string]int, len(topics))
	for _, topic := range topics {
		if partitionNum, err := c.client.Partitions(topic); err != nil {
			Logger.Printf(
				"consumergroup/%s topic %s get partition number failed due to '%v'\n",
				c.groupID, topic, err)
			return nil, err
		} else {
			topicToPartitionNum[topic] = len(partitionNum)
		}
	}
	return topicToPartitionNum, nil
}

// --------------------------------------------------------------------

// ConsumerGroupSession represents a consumer group member session.
type ConsumerGroupSession interface {
	// Claims returns information about the claimed partitions by topic.
	Claims() map[string][]int32

	// MemberID returns the cluster member ID.
	MemberID() string

	// GenerationID returns the current generation ID.
	GenerationID() int32

	// MarkOffset marks the provided offset, alongside a metadata string
	// that represents the state of the partition consumer at that point in time. The
	// metadata string can be used by another consumer to restore that state, so it
	// can resume consumption.
	//
	// To follow upstream conventions, you are expected to mark the offset of the
	// next message to read, not the last message read. Thus, when calling `MarkOffset`
	// you should typically add one to the offset of the last consumed message.
	//
	// Note: calling MarkOffset does not necessarily commit the offset to the backend
	// store immediately for efficiency reasons, and it may never be committed if
	// your application crashes. This means that you may end up processing the same
	// message twice, and your processing should ideally be idempotent.
	MarkOffset(topic string, partition int32, offset int64, metadata string)

	// Commit the offset to the backend
	//
	// Note: calling Commit performs a blocking synchronous operation.
	Commit()

	// ResetOffset resets to the provided offset, alongside a metadata string that
	// represents the state of the partition consumer at that point in time. Reset
	// acts as a counterpart to MarkOffset, the difference being that it allows to
	// reset an offset to an earlier or smaller value, where MarkOffset only
	// allows incrementing the offset. cf MarkOffset for more details.
	ResetOffset(topic string, partition int32, offset int64, metadata string)

	// MarkMessage marks a message as consumed.
	MarkMessage(msg *ConsumerMessage, metadata string)

	// Context returns the session context.
	Context() context.Context
}

type consumerGroupSession struct {
	parent       *consumerGroup
	memberID     string
	generationID int32
	handler      ConsumerGroupHandler

	claims  map[string][]int32
	offsets *offsetManager
	ctx     context.Context
	cancel  func()

	waitGroup       sync.WaitGroup
	releaseOnce     sync.Once
	hbDying, hbDead chan none
}

func newConsumerGroupSession(ctx context.Context, parent *consumerGroup, claims map[string][]int32, memberID string, generationID int32, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	// init offset manager
	// 获取一个新的offsetManager管理器
	offsets, err := newOffsetManagerFromClient(parent.groupID, memberID, generationID, parent.client)
	if err != nil {
		return nil, err
	}

	// 这里方便及时关闭所有的群组
	ctx, cancel := context.WithCancel(ctx)

	// init session
	sess := &consumerGroupSession{
		parent:       parent,
		memberID:     memberID,
		generationID: generationID,
		handler:      handler,
		offsets:      offsets,
		claims:       claims,
		ctx:          ctx,
		cancel:       cancel,
		hbDying:      make(chan none),
		hbDead:       make(chan none),
	}

	// start heartbeat loop
	// 异步心跳检测，保证协调器和consumer的通信，检测consumer存活
	go sess.heartbeatLoop()

	// create a POM for each claim， 这里claims是再平衡之后的
	// 创建每一个claim的对象模型
	for topic, partitions := range claims {
		for _, partition := range partitions {
			// 对主题和分区进行管理
			pom, err := offsets.ManagePartition(topic, partition)
			if err != nil {
				_ = sess.release(false)
				return nil, err
			}

			// handle POM errors
			go func(topic string, partition int32) {
				for err := range pom.Errors() {
					sess.parent.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}

	// perform setup
	// consumer从broker获取消息之前调用handler.Setup,来通知调用者做必要的处理
	if err := handler.Setup(sess); err != nil {
		_ = sess.release(true)
		return nil, err
	}

	// 消费拉取所有的数据
	for topic, partitions := range claims {

		// 每个成员分配的主题和分区，异步拉取
		for _, partition := range partitions {
			sess.waitGroup.Add(1)

			go func(topic string, partition int32) {
				defer sess.waitGroup.Done()

				// 拉取第一个分区之后立即取消会话
				defer sess.cancel()

				// 阻塞拉取，单个主题或者分区
				sess.consume(topic, partition)
			}(topic, partition) // => 以分区为维度进行异步拉取
		}
	}
	return sess, nil
}

func (s *consumerGroupSession) Claims() map[string][]int32 { return s.claims }
func (s *consumerGroupSession) MemberID() string           { return s.memberID }
func (s *consumerGroupSession) GenerationID() int32        { return s.generationID }

func (s *consumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.MarkOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) Commit() {
	s.offsets.Commit()
}

func (s *consumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.ResetOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) MarkMessage(msg *ConsumerMessage, metadata string) {
	s.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

func (s *consumerGroupSession) Context() context.Context {
	return s.ctx
}

func (s *consumerGroupSession) consume(topic string, partition int32) {
	// quick exit if rebalance is due
	// 正在在平衡直接退出
	select {
	case <-s.ctx.Done():
		return
	case <-s.parent.closed:
		return
	default:
	}

	// 获取需要拉取message的offset
	offset := s.parent.config.Consumer.Offsets.Initial
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		offset, _ = pom.NextOffset()
	}

	// 创建一个新的claim并订阅主题，拉取数据
	claim, err := newConsumerGroupClaim(s, topic, partition, offset)
	if err != nil {
		s.parent.handleError(err, topic, partition)
		return
	}

	// handle errors
	go func() {
		for err := range claim.Errors() {
			s.parent.handleError(err, topic, partition)
		}
	}()

	// trigger close when session is done
	go func() {
		select {
		case <-s.ctx.Done():
		case <-s.parent.closed:
		}
		claim.AsyncClose()
	}()

	// 拉取的消息传入handler.ConsumeClaim函数处理
	if err := s.handler.ConsumeClaim(s, claim); err != nil {
		s.parent.handleError(err, topic, partition)
	}

	// 数据处理完毕，异步关闭，处理遗留的错误和元信息
	claim.AsyncClose()
	for _, err := range claim.waitClosed() {
		s.parent.handleError(err, topic, partition)
	}
}

func (s *consumerGroupSession) release(withCleanup bool) (err error) {
	// signal release, stop heartbeat
	s.cancel()

	// wait for consumers to exit
	s.waitGroup.Wait()

	// perform release
	s.releaseOnce.Do(func() {
		// 本次执行完毕
		if withCleanup {
			if e := s.handler.Cleanup(s); e != nil {
				s.parent.handleError(e, "", -1)
				err = e
			}
		}

		if e := s.offsets.Close(); e != nil {
			err = e
		}

		close(s.hbDying)
		<-s.hbDead
	})

	Logger.Printf(
		"consumergroup/session/%s/%d released\n",
		s.MemberID(), s.GenerationID())

	return
}

// 检测consumer的是否可达，这里使用Group.Heartbeat.Interval作为健康检查的时间
// 使用Metadata.Retry.Backoff作为阻塞的时间，如果阻塞时间到retries--
// 如果连接关闭直接返回
// 发生错误重试
func (s *consumerGroupSession) heartbeatLoop() {
	defer close(s.hbDead)
	defer s.cancel() // trigger the end of the session on exit
	defer func() {
		Logger.Printf(
			"consumergroup/session/%s/%d heartbeat loop stopped\n",
			s.MemberID(), s.GenerationID())
	}()

	pause := time.NewTicker(s.parent.config.Consumer.Group.Heartbeat.Interval)
	defer pause.Stop()

	retryBackoff := time.NewTimer(s.parent.config.Metadata.Retry.Backoff)
	defer retryBackoff.Stop()

	retries := s.parent.config.Metadata.Retry.Max
	for {
		coordinator, err := s.parent.client.Coordinator(s.parent.groupID)
		if err != nil {
			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}
			retryBackoff.Reset(s.parent.config.Metadata.Retry.Backoff)
			select {
			case <-s.hbDying:
				return
			case <-retryBackoff.C:
				retries--
			}
			continue
		}

		resp, err := s.parent.heartbeatRequest(coordinator, s.memberID, s.generationID)
		if err != nil {
			_ = coordinator.Close()

			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}

			retries--
			continue
		}

		switch resp.Err {
		case ErrNoError:
			retries = s.parent.config.Metadata.Retry.Max
		case ErrRebalanceInProgress:
			retries = s.parent.config.Metadata.Retry.Max
			s.cancel()
		case ErrUnknownMemberId, ErrIllegalGeneration:
			return
		default:
			s.parent.handleError(resp.Err, "", -1)
			return
		}

		select {
		case <-pause.C:
		case <-s.hbDying:
			return
		}
	}
}

// --------------------------------------------------------------------

// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type ConsumerGroupHandler interface {
	// 在session开始之前调用
	Setup(ConsumerGroupSession) error

	// 清理在会话结束时运行，一旦所有ConsumerClaim Goroutine退出，但在最后一次提交偏移量之前。
	Cleanup(ConsumerGroupSession) error

	// ConsumerClaim必须启动ConsumerGroupClaim消息（）的消费者循环。
	// 一旦Messages（）通道关闭，处理程序必须完成其处理循环并退出。
	ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
}

// ConsumerGroupClaim processes Kafka messages from a given topic and partition within a consumer group.
type ConsumerGroupClaim interface {
	// Topic returns the consumed topic name.
	Topic() string

	// Partition returns the consumed partition.
	Partition() int32

	// InitialOffset returns the initial offset that was used as a starting point for this claim.
	InitialOffset() int64

	// HighWaterMarkOffset returns the high water mark offset of the partition,
	// i.e. the offset that will be used for the next message that will be produced.
	// You can use this to determine how far behind the processing is.
	HighWaterMarkOffset() int64

	// Messages returns the read channel for the messages that are returned by
	// the broker. The messages channel will be closed when a new rebalance cycle
	// is due. You must finish processing and mark offsets within
	// Config.Consumer.Group.Session.Timeout before the topic/partition is eventually
	// re-assigned to another group member.
	Messages() <-chan *ConsumerMessage
}

type consumerGroupClaim struct {
	topic     string
	partition int32
	offset    int64
	PartitionConsumer
}

func newConsumerGroupClaim(sess *consumerGroupSession, topic string, partition int32, offset int64) (*consumerGroupClaim, error) {
	// 订阅主题，开启监控，持续拉取指定分区的数据
	pcm, err := sess.parent.consumer.ConsumePartition(topic, partition, offset)
	if errors.Is(err, ErrOffsetOutOfRange) {
		offset = sess.parent.config.Consumer.Offsets.Initial
		pcm, err = sess.parent.consumer.ConsumePartition(topic, partition, offset)
	}

	if err != nil {
		return nil, err
	}

	go func() {
		for err := range pcm.Errors() {
			sess.parent.handleError(err, topic, partition)
		}
	}()

	return &consumerGroupClaim{
		topic:             topic,
		partition:         partition,
		offset:            offset,
		PartitionConsumer: pcm,
	}, nil
}

func (c *consumerGroupClaim) Topic() string        { return c.topic }
func (c *consumerGroupClaim) Partition() int32     { return c.partition }
func (c *consumerGroupClaim) InitialOffset() int64 { return c.offset }

// Drains messages and errors, ensures the claim is fully closed.
func (c *consumerGroupClaim) waitClosed() (errs ConsumerErrors) {
	go func() {
		for range c.Messages() {
		}
	}()

	for err := range c.Errors() {
		errs = append(errs, err)
	}
	return
}
