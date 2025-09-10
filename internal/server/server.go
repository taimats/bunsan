package server

import (
	"context"

	"connectrpc.com/connect"
	logv1 "github.com/taimats/bunsan/gen/log/v1"
	"github.com/taimats/bunsan/gen/log/v1/logv1connect"
)

var _ logv1connect.LogServiceHandler = (*grpcServer)(nil)

type Config struct {
	CommitLog CommitLog
}

type CommitLog interface {
	Append(*logv1.Record) (uint64, error)
	Read(uint64) (*logv1.Record, error)
}

type grpcServer struct {
	*Config
}

func newgrpcServer(c *Config) *grpcServer {
	return &grpcServer{Config: c}
}

func (s *grpcServer) Produce(ctx context.Context, r *connect.Request[logv1.ProduceRequest]) (*connect.Response[logv1.ProduceResponse], error) {
	off, err := s.CommitLog.Append(r.Msg.Record)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&logv1.ProduceResponse{Offset: off}), nil
}

func (s *grpcServer) Consume(ctx context.Context, r *connect.Request[logv1.ConsumeRequest]) (*connect.Response[logv1.ConsumeResponse], error) {
	record, err := s.CommitLog.Read(r.Msg.Offset)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&logv1.ConsumeResponse{Record: record}), nil
}
func (s *grpcServer) ProduceBidiStream(
	ctx context.Context,
	stream *connect.BidiStream[logv1.ProduceBidiStreamRequest, logv1.ProduceBidiStreamResponse],
) error {
	for {
		req, err := stream.Receive()
		if err != nil {
			return err
		}
		off, err := s.CommitLog.Append(req.Record)
		if err != nil {
			return err
		}
		res := &logv1.ProduceBidiStreamResponse{Offset: off}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}
func (s *grpcServer) ConsumeServerStream(
	ctx context.Context,
	req *connect.Request[logv1.ConsumeServerStreamRequest],
	stream *connect.ServerStream[logv1.ConsumeServerStreamResponse],
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			record, err := s.CommitLog.Read(req.Msg.Offset)
			if err != nil {
				return err
			}
			res := &logv1.ConsumeServerStreamResponse{Record: record}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Msg.Offset++
		}
	}
}
