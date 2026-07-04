package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/digest"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// redactPrivateAuthMetadata returns a copy of auxiliaryMetadata in which
// every AuthenticationMetadata Any has its `private` field cleared.
// `public` and `tracing_attributes` are preserved; non-auth Anys pass
// through unchanged. The scheduler ships the full AuthenticationMetadata
// (including `private`) to the worker for in-worker propagation (e.g.
// via add_metadata_jmespath_expression on the outbound runner-client
// interceptor), but the documented semantic of `private` is "not for
// public display" — so the worker must redact it before echoing the
// auxiliary_metadata back in the ExecuteResponse, where it would
// otherwise reach CAS, action cache, bb_browser, and CompletedActionLogger.
func redactPrivateAuthMetadata(auxiliaryMetadata []*anypb.Any) []*anypb.Any {
	result := make([]*anypb.Any, 0, len(auxiliaryMetadata))
	for _, am := range auxiliaryMetadata {
		if !am.MessageIs(&auth_pb.AuthenticationMetadata{}) {
			result = append(result, am)
			continue
		}
		var authMD auth_pb.AuthenticationMetadata
		if err := am.UnmarshalTo(&authMD); err != nil {
			// Advertised as AuthenticationMetadata but unparseable —
			// omit rather than risk passing through unredacted bytes.
			continue
		}
		authMD.Private = nil
		stripped, err := anypb.New(&authMD)
		if err != nil {
			// Re-marshal of a successfully unmarshalled message
			// shouldn't fail in practice; omit on the off chance.
			continue
		}
		result = append(result, stripped)
	}
	return result
}

// NewDefaultExecuteResponse creates an ExecuteResponse message that
// contains all fields that BuildExecutor should set by default.
//
// The request's auxiliary_metadata is echoed into the response with the
// `private` field of any AuthenticationMetadata Any redacted — see
// redactPrivateAuthMetadata.
func NewDefaultExecuteResponse(request *remoteworker.DesiredState_Executing) *remoteexecution.ExecuteResponse {
	return &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				AuxiliaryMetadata: redactPrivateAuthMetadata(request.AuxiliaryMetadata),
			},
		},
		ServerLogs: map[string]*remoteexecution.LogFile{},
	}
}

// attachErrorToExecuteResponse extends an ExecuteResponse to contain an
// error, indicating that the action has failed. If the ExecuteResponse
// already contains an error, it is not overwritten. This is done,
// because the first error is typically the most interesting one to
// return the user. As successive errors may well be related to the
// first, returning all of them would be noisy.
func attachErrorToExecuteResponse(response *remoteexecution.ExecuteResponse, err error) {
	if status.ErrorProto(response.Status) == nil {
		response.Status = status.Convert(err).Proto()
	}
}

func executeResponseIsSuccessful(response *remoteexecution.ExecuteResponse) bool {
	return status.ErrorProto(response.Status) == nil && response.Result.ExitCode == 0
}

// GetResultAndGRPCCodeFromExecuteResponse converts an ExecuteResponse
// to a pair of strings that describe the execution outcome. These
// strings can be used as part of metrics labels.
//
// TODO: Move this into some other package, so that pkg/scheduler
// doesn't need to depend on pkg/builder?
func GetResultAndGRPCCodeFromExecuteResponse(response *remoteexecution.ExecuteResponse) (result, grpcCode string) {
	if c := status.FromProto(response.Status).Code(); c != codes.OK {
		result = "Failure"
		grpcCode = c.String()
	} else if actionResult := response.Result; actionResult == nil {
		result = "ActionResultMissing"
	} else if actionResult.ExitCode == 0 {
		result = "Success"
	} else {
		result = "NonZeroExitCode"
	}
	return result, grpcCode
}

// BuildExecutor is the interface for the ability to run Bazel execute
// requests and yield an execute response.
type BuildExecutor interface {
	CheckReadiness(ctx context.Context) error
	Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse
}
