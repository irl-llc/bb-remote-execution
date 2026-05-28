package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/auth"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestContextWithAuthenticationMetadataFromAuxiliary covers the worker-side
// reconstitution of AuthenticationMetadata from auxiliary_metadata (the
// symmetric counterpart to the scheduler-side pack in
// pkg/scheduler/in_memory_build_queue.go).
func TestContextWithAuthenticationMetadataFromAuxiliary(t *testing.T) {
	// Sentinel "default" — what AuthenticationMetadataFromContext returns
	// when the ctx has no metadata attached.
	defaultMD := auth.AuthenticationMetadataFromContext(context.Background())

	t.Run("NoAuxiliaryMetadataIsNoOp", func(t *testing.T) {
		ctx := builder.ContextWithAuthenticationMetadataFromAuxiliary(context.Background(), nil)
		require.Same(t, defaultMD, auth.AuthenticationMetadataFromContext(ctx))
	})

	t.Run("OnlyNonAuthAnyIsNoOp", func(t *testing.T) {
		nonAuth, err := anypb.New(&remoteexecution.Digest{Hash: "abc", SizeBytes: 1})
		require.NoError(t, err)
		ctx := builder.ContextWithAuthenticationMetadataFromAuxiliary(
			context.Background(),
			[]*anypb.Any{nonAuth},
		)
		require.Same(t, defaultMD, auth.AuthenticationMetadataFromContext(ctx))
	})

	t.Run("AuthAnyReconstitutesOnContext", func(t *testing.T) {
		authAny, err := anypb.New(&auth_pb.AuthenticationMetadata{
			Public:  structpb.NewStringValue("alice@example.com"),
			Private: structpb.NewStringValue("bearer-xyz"),
		})
		require.NoError(t, err)

		ctx := builder.ContextWithAuthenticationMetadataFromAuxiliary(
			context.Background(),
			[]*anypb.Any{authAny},
		)
		got := auth.AuthenticationMetadataFromContext(ctx)
		require.NotSame(t, defaultMD, got, "expected reconstituted metadata, got default")

		fullProto := got.GetFullProto()
		require.Equal(t, structpb.NewStringValue("alice@example.com"), fullProto.Public)
		require.Equal(t, structpb.NewStringValue("bearer-xyz"), fullProto.Private)
	})

	t.Run("MalformedAuthAnyIsNoOp", func(t *testing.T) {
		malformed := &anypb.Any{
			TypeUrl: "type.googleapis.com/buildbarn.auth.AuthenticationMetadata",
			Value:   []byte{0xff, 0xff, 0xff},
		}
		ctx := builder.ContextWithAuthenticationMetadataFromAuxiliary(
			context.Background(),
			[]*anypb.Any{malformed},
		)
		require.Same(t, defaultMD, auth.AuthenticationMetadataFromContext(ctx))
	})

	t.Run("FirstAuthAnyWins", func(t *testing.T) {
		// Defense-in-depth: if (somehow) multiple AuthenticationMetadata
		// Anys appear in auxiliary_metadata, the helper attaches the
		// first one and stops scanning — predictable, not ambiguous.
		first, err := anypb.New(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("first"),
		})
		require.NoError(t, err)
		second, err := anypb.New(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("second"),
		})
		require.NoError(t, err)

		ctx := builder.ContextWithAuthenticationMetadataFromAuxiliary(
			context.Background(),
			[]*anypb.Any{first, second},
		)
		got := auth.AuthenticationMetadataFromContext(ctx)
		require.Equal(t, structpb.NewStringValue("first"), got.GetFullProto().Public)
	})
}
