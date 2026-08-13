// Package runtimeroster validates one ready Media Edge and the exact set of
// ready camera-source process receipts that feed it.
package runtimeroster

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strings"

	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	protocol "github.com/lxk36/xgc2-orchestration-core/kernel/node"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const packageDigest = "sha256:2040b724bb1c08ee10fc03f4440b6118b18f878f91f134c60441e0376f08c5f6"

const maximumSources = 16

type Executor struct{ descriptor contracts.NodeDescriptor }

func New() *Executor {
	stringSchema := contracts.Schema{Type: contracts.TypeString}
	staticSource := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "sourceId": stringSchema,
		"imageTopic": stringSchema, "cameraInfoTopic": stringSchema,
	}, Required: []string{"bindingId", "sourceId", "imageTopic", "cameraInfoTopic"}}
	readySource := contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
		"bindingId": stringSchema, "sourceId": stringSchema, "cameraExternalIdentity": stringSchema,
		"imageTopic": stringSchema, "cameraInfoTopic": stringSchema, "streamUrl": stringSchema,
	}, Required: []string{
		"bindingId", "sourceId", "cameraExternalIdentity", "imageTopic", "cameraInfoTopic", "streamUrl",
	}}
	descriptor := contracts.NodeDescriptor{
		SchemaVersion: protocol.DescriptorSchemaVersion,
		TypeRef:       "xgc.media.runtime-roster-assert/v2", DisplayName: "Ready media source roster assertion",
		PackageRef: "xgc2-nodes-media", PackageDigest: packageDigest,
		InputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"edgeBindingId": stringSchema, "edgeUrl": stringSchema, "edgeExternalIdentity": stringSchema,
			"rosterArtifactRef": stringSchema, "rosterDigest": stringSchema,
			"sources":                  {Type: contracts.TypeArray, Items: &staticSource},
			"cameraExternalIdentities": {Type: contracts.TypeArray, Items: &stringSchema},
		}, Required: []string{
			"edgeBindingId", "edgeUrl", "edgeExternalIdentity", "rosterArtifactRef", "rosterDigest",
			"sources", "cameraExternalIdentities",
		}},
		OutputSchema: contracts.Schema{Type: contracts.TypeObject, Properties: map[string]contracts.Schema{
			"edgeBindingId": stringSchema, "edgeUrl": stringSchema, "edgeExternalIdentity": stringSchema,
			"rosterArtifactRef": stringSchema, "rosterDigest": stringSchema,
			"sources": {Type: contracts.TypeArray, Items: &readySource},
			"count":   {Type: contracts.TypeInteger}, "matched": {Type: contracts.TypeBoolean},
		}, Required: []string{
			"edgeBindingId", "edgeUrl", "edgeExternalIdentity", "rosterArtifactRef", "rosterDigest",
			"sources", "count", "matched",
		}},
		Mode: contracts.NodePure, Determinism: contracts.NodeDeterministic,
		MaxInputBytes: 262144, MaxOutputBytes: 262144,
	}
	descriptor.DescriptorDigest, _ = protocol.DescriptorDigest(descriptor)
	return &Executor{descriptor: descriptor}
}

func (executor *Executor) Descriptor() contracts.NodeDescriptor { return executor.descriptor }

func (executor *Executor) Execute(_ context.Context, request contracts.NodeInvocationRequest) (contracts.NodeResult, error) {
	edgeBindingID, _ := request.Input["edgeBindingId"].(string)
	edgeURL, _ := request.Input["edgeUrl"].(string)
	edgeExternalIdentity, _ := request.Input["edgeExternalIdentity"].(string)
	rosterArtifactRef, _ := request.Input["rosterArtifactRef"].(string)
	rosterDigest, _ := request.Input["rosterDigest"].(string)
	if !contracts.ValidIdentifier(edgeBindingID) || !contracts.ValidIdentifier(edgeExternalIdentity) ||
		!contracts.ValidIdentifier(rosterArtifactRef) || !contracts.ValidDigest(rosterDigest) || !validEdgeURL(edgeURL) {
		return failed("media-roster.edge-invalid", errors.New("media edge binding, URL, runtime identity, or roster artifact identity is invalid")), nil
	}
	sources, err := normalizeSources(request.Input["sources"], request.Input["cameraExternalIdentities"], edgeURL, edgeExternalIdentity)
	if err != nil {
		return failed("media-roster.sources-invalid", err), nil
	}
	output := map[string]any{
		"edgeBindingId": edgeBindingID, "edgeUrl": strings.TrimRight(edgeURL, "/"),
		"edgeExternalIdentity": edgeExternalIdentity,
		"rosterArtifactRef":    rosterArtifactRef, "rosterDigest": rosterDigest, "sources": sources,
		"count": int64(len(sources)), "matched": true,
	}
	digest, err := canonicaljson.DigestValue(output)
	if err != nil {
		return contracts.NodeResult{}, err
	}
	return contracts.NodeResult{Status: contracts.NodeResultSucceeded, Output: output, OutputDigest: digest, EvidenceDigest: digest}, nil
}

func normalizeSources(sourcesValue, identitiesValue any, edgeURL, edgeExternalIdentity string) ([]any, error) {
	raw, ok := sourcesValue.([]any)
	if !ok || len(raw) == 0 || len(raw) > maximumSources {
		return nil, errors.New("media source roster must contain between one and sixteen sources")
	}
	identities, ok := identitiesValue.([]any)
	if !ok || len(identities) != len(raw) {
		return nil, errors.New("one camera runtime identity is required per media source")
	}
	seenBindings := make(map[string]struct{}, len(raw))
	seenSources := make(map[string]struct{}, len(raw))
	seenExternal := map[string]struct{}{edgeExternalIdentity: {}}
	result := make([]any, 0, len(raw))
	for index, item := range raw {
		source, sourceOK := item.(map[string]any)
		cameraExternal, identityOK := identities[index].(string)
		if !sourceOK || !identityOK || !contracts.ValidIdentifier(cameraExternal) {
			return nil, errors.New("media source or camera runtime identity is invalid")
		}
		bindingID, bindingOK := source["bindingId"].(string)
		sourceID, sourceIDOK := source["sourceId"].(string)
		imageTopic, imageOK := source["imageTopic"].(string)
		cameraInfoTopic, infoOK := source["cameraInfoTopic"].(string)
		_, duplicateBinding := seenBindings[bindingID]
		_, duplicateSource := seenSources[sourceID]
		_, duplicateExternal := seenExternal[cameraExternal]
		if !bindingOK || !sourceIDOK || !imageOK || !infoOK ||
			!contracts.ValidIdentifier(bindingID) || !contracts.ValidIdentifier(sourceID) ||
			!validTopic(imageTopic) || !validTopic(cameraInfoTopic) || duplicateBinding || duplicateSource ||
			duplicateExternal {
			return nil, errors.New("media source identities or ROS topics are invalid or duplicated")
		}
		seenBindings[bindingID] = struct{}{}
		seenSources[sourceID] = struct{}{}
		seenExternal[cameraExternal] = struct{}{}
		result = append(result, map[string]any{
			"bindingId": bindingID, "sourceId": sourceID, "cameraExternalIdentity": cameraExternal,
			"imageTopic": imageTopic, "cameraInfoTopic": cameraInfoTopic,
			"streamUrl": strings.TrimRight(edgeURL, "/") + "/sources/" + url.PathEscape(sourceID),
		})
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].(map[string]any)["bindingId"].(string) < result[right].(map[string]any)["bindingId"].(string)
	})
	return result, nil
}

func validEdgeURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" &&
		parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" && (parsed.Path == "" || parsed.Path == "/")
}

func validTopic(value string) bool {
	return strings.HasPrefix(value, "/") && value != "/" && !strings.Contains(value, "//") && !strings.ContainsAny(value, " \t\r\n")
}

func failed(code string, cause error) contracts.NodeResult {
	failure := &contracts.StructuredFailure{Class: contracts.FailurePermanent, Code: code, Message: cause.Error()}
	evidence, _ := canonicaljson.DigestValue(failure)
	return contracts.NodeResult{Status: contracts.NodeResultFailed, Failure: failure, EvidenceDigest: evidence}
}
