package runtimeroster

import (
	"testing"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

func TestRuntimeRosterAcceptsSeveralExactReadySources(t *testing.T) {
	executor := New()
	input := validInput()
	digest, _ := canonicaljson.DigestValue(input)
	result, err := executor.Execute(t.Context(), contracts.NodeInvocationRequest{
		Input: input, InputDigest: digest, RequestedAt: time.Now().UTC(), Deadline: time.Now().UTC().Add(time.Minute),
	})
	if err != nil || result.Status != contracts.NodeResultSucceeded || result.Output["count"] != int64(2) {
		t.Fatalf("runtime roster result=%#v err=%v", result, err)
	}
	sources, ok := result.Output["sources"].([]any)
	if !ok || len(sources) != 2 || sources[1].(map[string]any)["streamUrl"] != "http://127.0.0.1:28090/sources/world" {
		t.Fatalf("runtime sources=%#v", result.Output["sources"])
	}
	for _, source := range sources {
		projected := source.(map[string]any)
		if _, leaked := projected["controlSocket"]; leaked {
			t.Fatalf("runtime roster leaked a control socket: %#v", projected)
		}
		if _, leaked := projected["rtpPort"]; leaked {
			t.Fatalf("runtime roster leaked an RTP port: %#v", projected)
		}
	}
}

func TestRuntimeRosterRejectsMisalignmentAndEveryDuplicateAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing receipt", mutate: func(input map[string]any) {
			input["cameraExternalIdentities"] = input["cameraExternalIdentities"].([]any)[:1]
		}},
		{name: "duplicate source", mutate: func(input map[string]any) {
			input["sources"].([]any)[1].(map[string]any)["sourceId"] = "front"
		}},
		{name: "duplicate runtime", mutate: func(input map[string]any) {
			input["cameraExternalIdentities"].([]any)[1] = "camera-front-process"
		}},
		{name: "edge reused as camera", mutate: func(input map[string]any) {
			input["cameraExternalIdentities"].([]any)[1] = "media-edge-process"
		}},
	}
	executor := New()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := cloneInput(validInput())
			test.mutate(input)
			result, err := executor.Execute(t.Context(), contracts.NodeInvocationRequest{Input: input})
			if err != nil || result.Status != contracts.NodeResultFailed || result.Failure == nil || result.Failure.Code != "media-roster.sources-invalid" {
				t.Fatalf("runtime roster result=%#v err=%v", result, err)
			}
		})
	}
}

func validInput() map[string]any {
	return map[string]any{
		"edgeBindingId": "media-edge", "edgeUrl": "http://127.0.0.1:28090", "edgeExternalIdentity": "media-edge-process",
		"rosterArtifactRef": "camera-source-roster-fixture", "rosterDigest": "sha256:613f37ed7bedc20b3933fdb6ff08f56f677160379899de45d9ab09aa06839221",
		"sources": []any{
			map[string]any{
				"bindingId": "camera-front", "sourceId": "front", "imageTopic": "/xgc/camera/front/video_h264",
				"cameraInfoTopic": "/xgc/camera/front/camera_info",
			},
			map[string]any{
				"bindingId": "camera-world", "sourceId": "world", "imageTopic": "/xgc/camera/world/video_h264",
				"cameraInfoTopic": "/xgc/camera/world/camera_info",
			},
		},
		"cameraExternalIdentities": []any{"camera-front-process", "camera-world-process"},
	}
}

func cloneInput(input map[string]any) map[string]any {
	raw, _ := canonicaljson.Marshal(input)
	var clone map[string]any
	_ = canonicaljson.UnmarshalStrict(raw, &clone)
	return clone
}
