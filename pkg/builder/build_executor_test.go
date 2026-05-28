package builder_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestNewDefaultExecuteResponseAuthMetadataRedaction verifies that
// AuthenticationMetadata Anys in the request's auxiliary_metadata have
// their `private` field stripped before being echoed into the response.
// Non-auth Anys must pass through unchanged. See redactPrivateAuthMetadata.
func TestNewDefaultExecuteResponseAuthMetadataRedaction(t *testing.T) {
	// A non-auth Any (use a Digest as a stand-in arbitrary message).
	nonAuthMessage := &remoteexecution.Digest{Hash: "deadbeef", SizeBytes: 42}
	nonAuthAny, err := anypb.New(nonAuthMessage)
	require.NoError(t, err)

	t.Run("EmptyAuxiliaryMetadata", func(t *testing.T) {
		response := builder.NewDefaultExecuteResponse(&remoteworker.DesiredState_Executing{})
		require.Empty(t, response.Result.ExecutionMetadata.AuxiliaryMetadata)
	})

	t.Run("NonAuthAnyPassesThrough", func(t *testing.T) {
		response := builder.NewDefaultExecuteResponse(&remoteworker.DesiredState_Executing{
			AuxiliaryMetadata: []*anypb.Any{nonAuthAny},
		})
		require.Len(t, response.Result.ExecutionMetadata.AuxiliaryMetadata, 1)
		testutil.RequireEqualProto(t, nonAuthAny, response.Result.ExecutionMetadata.AuxiliaryMetadata[0])
	})

	t.Run("AuthAnyPrivateRedactedPublicPreserved", func(t *testing.T) {
		authAny, err := anypb.New(&auth_pb.AuthenticationMetadata{
			Public:  structpb.NewStringValue("alice@example.com"),
			Private: structpb.NewStringValue("super-secret-bearer-token"),
		})
		require.NoError(t, err)

		response := builder.NewDefaultExecuteResponse(&remoteworker.DesiredState_Executing{
			AuxiliaryMetadata: []*anypb.Any{authAny},
		})
		require.Len(t, response.Result.ExecutionMetadata.AuxiliaryMetadata, 1)

		var got auth_pb.AuthenticationMetadata
		require.NoError(t, response.Result.ExecutionMetadata.AuxiliaryMetadata[0].UnmarshalTo(&got))
		require.Equal(t, structpb.NewStringValue("alice@example.com"), got.Public)
		require.Nil(t, got.Private, "private must be redacted from response")
	})

	t.Run("AuthAnyTracingAttributesPreserved", func(t *testing.T) {
		// Tracing attributes are part of GetFullProto and should be
		// preserved through the response (only Private is redacted).
		authAny, err := anypb.New(&auth_pb.AuthenticationMetadata{
			Public:  structpb.NewStringValue("alice"),
			Private: structpb.NewStringValue("secret"),
		})
		require.NoError(t, err)

		response := builder.NewDefaultExecuteResponse(&remoteworker.DesiredState_Executing{
			AuxiliaryMetadata: []*anypb.Any{authAny},
		})
		require.Len(t, response.Result.ExecutionMetadata.AuxiliaryMetadata, 1)
		var got auth_pb.AuthenticationMetadata
		require.NoError(t, response.Result.ExecutionMetadata.AuxiliaryMetadata[0].UnmarshalTo(&got))
		require.NotNil(t, got.Public)
		require.Nil(t, got.Private)
	})

	t.Run("MixedAnysOrderPreserved", func(t *testing.T) {
		authAny, err := anypb.New(&auth_pb.AuthenticationMetadata{
			Public:  structpb.NewStringValue("alice"),
			Private: structpb.NewStringValue("secret"),
		})
		require.NoError(t, err)

		response := builder.NewDefaultExecuteResponse(&remoteworker.DesiredState_Executing{
			AuxiliaryMetadata: []*anypb.Any{nonAuthAny, authAny, nonAuthAny},
		})
		require.Len(t, response.Result.ExecutionMetadata.AuxiliaryMetadata, 3)

		testutil.RequireEqualProto(t, nonAuthAny, response.Result.ExecutionMetadata.AuxiliaryMetadata[0])
		var got auth_pb.AuthenticationMetadata
		require.NoError(t, response.Result.ExecutionMetadata.AuxiliaryMetadata[1].UnmarshalTo(&got))
		require.Nil(t, got.Private)
		testutil.RequireEqualProto(t, nonAuthAny, response.Result.ExecutionMetadata.AuxiliaryMetadata[2])
	})

	t.Run("MalformedAuthAnyDropped", func(t *testing.T) {
		// An Any with the AuthenticationMetadata type URL but garbage
		// bytes is dropped (safer than passing through unredacted).
		malformed := &anypb.Any{
			TypeUrl: "type.googleapis.com/buildbarn.auth.AuthenticationMetadata",
			Value:   []byte{0xff, 0xff, 0xff, 0xff, 0xff},
		}
		response := builder.NewDefaultExecuteResponse(&remoteworker.DesiredState_Executing{
			AuxiliaryMetadata: []*anypb.Any{nonAuthAny, malformed, nonAuthAny},
		})
		require.Len(t, response.Result.ExecutionMetadata.AuxiliaryMetadata, 2)
		testutil.RequireEqualProto(t, nonAuthAny, response.Result.ExecutionMetadata.AuxiliaryMetadata[0])
		testutil.RequireEqualProto(t, nonAuthAny, response.Result.ExecutionMetadata.AuxiliaryMetadata[1])
	})
}
