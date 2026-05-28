package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"

	"google.golang.org/protobuf/types/known/anypb"
)

// ContextWithAuthenticationMetadataFromAuxiliary scans a worker's
// per-action auxiliary_metadata for an AuthenticationMetadata Any (the
// form that pkg/scheduler/in_memory_build_queue.go ships when
// dispatching DesiredState_Executing to a worker) and attaches the
// parsed metadata to ctx so that outbound gRPC interceptors — most
// notably add_metadata_jmespath_expression on the worker's runner-client
// connection — can read it from
// auth.AuthenticationMetadataFromContext(ctx) and propagate caller-scoped
// credentials/identity downstream.
//
// This is the symmetric counterpart of the scheduler-side pack. Returns
// ctx unchanged if no usable AuthenticationMetadata Any is present.
// Entries that advertise the AuthenticationMetadata type URL but fail to
// unmarshal are skipped (treated as not present); the first successfully
// parsed entry is attached and scanning stops.
func ContextWithAuthenticationMetadataFromAuxiliary(ctx context.Context, auxiliaryMetadata []*anypb.Any) context.Context {
	for _, am := range auxiliaryMetadata {
		if !am.MessageIs(&auth_pb.AuthenticationMetadata{}) {
			continue
		}
		var authMD auth_pb.AuthenticationMetadata
		if err := am.UnmarshalTo(&authMD); err != nil {
			continue
		}
		parsed, err := auth.NewAuthenticationMetadataFromProto(&authMD)
		if err != nil {
			continue
		}
		return auth.NewContextWithAuthenticationMetadata(ctx, parsed)
	}
	return ctx
}
