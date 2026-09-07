// Package nodegrpc talks to queue nodes. One connection per node, reused for
// every call and for the notification stream.
package nodegrpc

import (
	"context"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Repo struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func New() *Repo { return &Repo{conns: map[string]*grpc.ClientConn{}} }

func (r *Repo) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.conns {
		_ = c.Close()
	}
	r.conns = map[string]*grpc.ClientConn{}
}

func (r *Repo) client(addr string) (pb.QueueServiceClient, error) {
	r.mu.RLock()
	c, ok := r.conns[addr]
	r.mu.RUnlock()
	if ok {
		return pb.NewQueueServiceClient(c), nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.conns[addr]; ok {
		return pb.NewQueueServiceClient(c), nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, enterr.Wrap(enterr.CodeExhausted, "dial node", err)
	}
	r.conns[addr] = c
	return pb.NewQueueServiceClient(c), nil
}

// failed maps the error and, when the transport is the thing that broke, drops
// the cached connection.
func (r *Repo) failed(addr string, err error) error {
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		r.mu.Lock()
		if c, held := r.conns[addr]; held {
			delete(r.conns, addr)
			_ = c.Close()
		}
		r.mu.Unlock()
	}
	return fromStatus(err)
}

func fromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return enterr.Wrap(enterr.CodeInternal, "node call", err)
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return enterr.New(enterr.CodeInvalid, st.Message())
	case codes.NotFound:
		return enterr.New(enterr.CodeNotFound, st.Message())
	case codes.FailedPrecondition:
		return enterr.New(enterr.CodeConflict, st.Message())
	case codes.ResourceExhausted:
		return enterr.New(enterr.CodeExhausted, st.Message())
	case codes.Aborted:
		return enterr.New(enterr.CodeMoved, st.Message())
	case codes.Unavailable, codes.DeadlineExceeded:
		return enterr.New(enterr.CodeExhausted, "node unreachable: "+st.Message())
	}
	return enterr.New(enterr.CodeInternal, st.Message())
}

func specTo(s entity.QueueSpec) *pb.QueueSpec {
	return &pb.QueueSpec{
		Org: s.Org, Name: s.Name,
		VisibilityTimeoutNs:   int64(s.VisibilityTimeout),
		MaxRetries:            s.MaxRetries,
		DefaultTtlNs:          int64(s.DefaultTTL),
		StarvationThresholdNs: int64(s.StarvationThreshold),
		StarvationReserve:     s.StarvationReserve,
		MaxDepth:              s.MaxDepth,
		Distributed:           s.Distributed,
		HasDeadLetter:         s.HasDeadLetter,
		Generation:            s.Generation,
	}
}

func statsFrom(p *pb.QueueStats) entity.NodeStats {
	s := entity.NodeStats{
		Org: p.Org, Name: p.Name,
		InFlight: p.InFlight, Delayed: p.Delayed,
		OldestAge:    time.Duration(p.OldestAgeNs),
		Enqueued:     p.Enqueued,
		Acked:        p.Acked,
		Expired:      p.Expired,
		Requeued:     p.Requeued,
		DeadLettered: p.DeadLettered,
		Escapes:      p.Escapes,
	}
	for i := 0; i < len(p.Ready) && i < 3; i++ {
		s.Ready[i] = p.Ready[i]
	}
	return s
}

func (r *Repo) Enqueue(ctx context.Context, addr string, in entity.NodeEnqueue) (entity.NodeEnqueueResult, error) {
	c, err := r.client(addr)
	if err != nil {
		return entity.NodeEnqueueResult{}, err
	}
	req := &pb.EnqueueRequest{
		Spec: specTo(in.Spec), Payload: in.Payload, Priority: uint32(in.Priority),
		GroupId: in.GroupID, TtlNs: int64(in.TTL), DeliverAfterNs: int64(in.DeliverAfter),
	}
	if in.Slot != nil {
		slot := uint32(*in.Slot)
		req.Slot = &slot
	}
	out, err := c.Enqueue(ctx, req)
	if err != nil {
		return entity.NodeEnqueueResult{}, r.failed(addr, err)
	}
	return entity.NodeEnqueueResult{MessageID: out.MessageId, Slot: uint16(out.Slot), Seq: out.Seq}, nil
}

func (r *Repo) Dequeue(ctx context.Context, addr string, spec entity.QueueSpec, max int) ([]entity.NodeMessage, error) {
	c, err := r.client(addr)
	if err != nil {
		return nil, err
	}
	out, err := c.Dequeue(ctx, &pb.DequeueRequest{Spec: specTo(spec), MaxMessages: int32(max)})
	if err != nil {
		return nil, r.failed(addr, err)
	}
	msgs := make([]entity.NodeMessage, 0, len(out.Messages))
	for _, m := range out.Messages {
		msgs = append(msgs, entity.NodeMessage{
			MessageID: m.MessageId, Payload: m.Payload, Priority: uint8(m.Priority),
			GroupID: m.GroupId, Attempts: m.Attempts,
			EnqueuedAt: time.Unix(0, m.EnqueuedAtUnixNs).UTC(), Receipt: m.Receipt,
		})
	}
	return msgs, nil
}

func (r *Repo) Ack(ctx context.Context, addr string, spec entity.QueueSpec, receipt string) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	_, err = c.Ack(ctx, &pb.AckRequest{Spec: specTo(spec), Receipt: receipt})
	return r.failed(addr, err)
}

func (r *Repo) Nack(ctx context.Context, addr string, spec entity.QueueSpec, receipt string, delay time.Duration) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	_, err = c.Nack(ctx, &pb.NackRequest{Spec: specTo(spec), Receipt: receipt, DelayNs: int64(delay)})
	return r.failed(addr, err)
}

func (r *Repo) Stats(ctx context.Context, addr string, spec entity.QueueSpec) (entity.NodeStats, error) {
	c, err := r.client(addr)
	if err != nil {
		return entity.NodeStats{}, err
	}
	out, err := c.Stats(ctx, &pb.StatsRequest{Spec: specTo(spec)})
	if err != nil {
		return entity.NodeStats{}, r.failed(addr, err)
	}
	return statsFrom(out), nil
}

func (r *Repo) StatsAll(ctx context.Context, addr string) (string, []entity.NodeStats, error) {
	c, err := r.client(addr)
	if err != nil {
		return "", nil, err
	}
	out, err := c.StatsAll(ctx, &pb.Empty{})
	if err != nil {
		return "", nil, r.failed(addr, err)
	}
	stats := make([]entity.NodeStats, 0, len(out.Queues))
	for _, q := range out.Queues {
		stats = append(stats, statsFrom(q))
	}
	return out.NodeId, stats, nil
}

func (r *Repo) Drop(ctx context.Context, addr string, spec entity.QueueSpec) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	_, err = c.Drop(ctx, &pb.StatsRequest{Spec: specTo(spec)})
	return r.failed(addr, err)
}

func (r *Repo) Freeze(ctx context.Context, addr string, spec entity.QueueSpec, slots []uint16) (entity.Transfer, error) {
	c, err := r.client(addr)
	if err != nil {
		return entity.Transfer{}, err
	}
	only := make([]uint32, 0, len(slots))
	for _, s := range slots {
		only = append(only, uint32(s))
	}
	out, err := c.Freeze(ctx, &pb.FreezeRequest{Spec: specTo(spec), Slots: only})
	if err != nil {
		return entity.Transfer{}, r.failed(addr, err)
	}
	t := transferFrom(out)
	t.Spec = spec
	return t, nil
}

func transferFrom(out *pb.Transfer) entity.Transfer {
	t := entity.Transfer{BySlot: map[uint16][]entity.WireMessage{}}
	for slot, sm := range out.BySlot {
		msgs := make([]entity.WireMessage, 0, len(sm.Messages))
		for _, m := range sm.Messages {
			msgs = append(msgs, wireFrom(m))
		}
		t.BySlot[uint16(slot)] = msgs
	}
	return t
}

func (r *Repo) Absorb(ctx context.Context, addr string, t entity.Transfer) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	req := &pb.Transfer{Spec: specTo(t.Spec), MoveId: t.MoveID, BySlot: map[uint32]*pb.SlotMessages{}}
	for slot, msgs := range t.BySlot {
		sm := &pb.SlotMessages{Messages: make([]*pb.WireMessage, 0, len(msgs))}
		for _, m := range msgs {
			sm.Messages = append(sm.Messages, wireTo(m))
		}
		req.BySlot[uint32(slot)] = sm
	}
	_, err = c.Absorb(ctx, req)
	return r.failed(addr, err)
}

// Subscribe opens the notification stream.
func (r *Repo) Subscribe(ctx context.Context, addr, gatewayID string) (<-chan entity.WorkAvailable, error) {
	c, err := r.client(addr)
	if err != nil {
		return nil, err
	}
	stream, err := c.Subscribe(ctx, &pb.SubscribeRequest{GatewayId: gatewayID})
	if err != nil {
		return nil, r.failed(addr, err)
	}
	out := make(chan entity.WorkAvailable, 64)
	go func() {
		defer close(out)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case out <- entity.WorkAvailable{Org: msg.Org, Name: msg.Name}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func wireTo(m entity.WireMessage) *pb.WireMessage {
	w := &pb.WireMessage{
		Id: m.ID, Payload: m.Payload, Priority: uint32(m.Priority), GroupId: m.GroupID,
		Seq: m.Seq, EnqueuedAtUnixNs: m.EnqueuedAt.UnixNano(), Attempts: m.Attempts,
	}
	if m.ExpiresAt != nil {
		w.ExpiresAtUnixNs = m.ExpiresAt.UnixNano()
	}
	if m.DeliverAfter != nil {
		w.DeliverAfterUnixNs = m.DeliverAfter.UnixNano()
	}
	return w
}

func wireFrom(w *pb.WireMessage) entity.WireMessage {
	m := entity.WireMessage{
		ID: w.Id, Payload: w.Payload, Priority: uint8(w.Priority), GroupID: w.GroupId,
		Seq: w.Seq, EnqueuedAt: time.Unix(0, w.EnqueuedAtUnixNs).UTC(), Attempts: w.Attempts,
	}
	if w.ExpiresAtUnixNs != 0 {
		t := time.Unix(0, w.ExpiresAtUnixNs).UTC()
		m.ExpiresAt = &t
	}
	if w.DeliverAfterUnixNs != 0 {
		t := time.Unix(0, w.DeliverAfterUnixNs).UTC()
		m.DeliverAfter = &t
	}
	return m
}

func (r *Repo) PrepareMove(
	ctx context.Context, addr string, spec entity.QueueSpec, slots []uint16, moveID string,
) (entity.Transfer, error) {
	c, err := r.client(addr)
	if err != nil {
		return entity.Transfer{}, err
	}
	out, err := c.PrepareMove(ctx, &pb.SlotSet{
		Spec: specTo(spec), Slots: slotsTo(slots), MoveId: moveID,
	})
	if err != nil {
		return entity.Transfer{}, r.failed(addr, err)
	}
	return transferFrom(out), nil
}

func (r *Repo) DiscardMove(
	ctx context.Context, addr string, spec entity.QueueSpec, slots []uint16, moveID string,
) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	_, err = c.DiscardMove(ctx, &pb.SlotSet{
		Spec: specTo(spec), Slots: slotsTo(slots), MoveId: moveID,
	})
	return r.failed(addr, err)
}

func (r *Repo) AbortMove(
	ctx context.Context, addr string, spec entity.QueueSpec, slots []uint16, moveID string,
) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	_, err = c.AbortMove(ctx, &pb.SlotSet{
		Spec: specTo(spec), Slots: slotsTo(slots), MoveId: moveID,
	})
	return r.failed(addr, err)
}

func (r *Repo) Held(ctx context.Context, addr string) (entity.HeldResponse, error) {
	c, err := r.client(addr)
	if err != nil {
		return entity.HeldResponse{}, err
	}
	out, err := c.Held(ctx, &pb.Empty{})
	if err != nil {
		return entity.HeldResponse{}, r.failed(addr, err)
	}
	held := entity.HeldResponse{NodeID: out.NodeId}
	for _, q := range out.Queues {
		held.Queues = append(held.Queues, entity.HeldSlots{
			Org: q.Org, Name: q.Queue,
			Slots: slotsFrom(q.Slots), Frozen: slotsFrom(q.Frozen),
		})
	}
	return held, nil
}

func slotsTo(in []uint16) []uint32 {
	out := make([]uint32, 0, len(in))
	for _, v := range in {
		out = append(out, uint32(v))
	}
	return out
}

func slotsFrom(in []uint32) []uint16 {
	out := make([]uint16, 0, len(in))
	for _, v := range in {
		out = append(out, uint16(v))
	}
	return out
}

func (r *Repo) DeadLetters(ctx context.Context, addr string) ([]entity.DeadLetterItem, error) {
	c, err := r.client(addr)
	if err != nil {
		return nil, err
	}
	out, err := c.DeadLetters(ctx, &pb.Empty{})
	if err != nil {
		return nil, r.failed(addr, err)
	}
	items := make([]entity.DeadLetterItem, 0, len(out.DeadLetters))
	for _, d := range out.DeadLetters {
		m := d.GetMessage()
		if m == nil {
			continue
		}
		items = append(items, entity.DeadLetterItem{
			Org:  d.Org,
			Name: d.Name,
			Message: entity.NodeMessage{
				MessageID:  m.Id,
				Payload:    m.Payload,
				Priority:   uint8(m.Priority),
				GroupID:    m.GroupId,
				Attempts:   m.Attempts,
				EnqueuedAt: time.Unix(0, m.EnqueuedAtUnixNs),
			},
			Attempts: m.Attempts,
		})
	}
	return items, nil
}

func (r *Repo) AckDeadLetters(ctx context.Context, addr string, ids []string) error {
	c, err := r.client(addr)
	if err != nil {
		return err
	}
	if _, err := c.AckDeadLetters(ctx, &pb.AckDeadLettersRequest{MessageIds: ids}); err != nil {
		return r.failed(addr, err)
	}
	return nil
}
