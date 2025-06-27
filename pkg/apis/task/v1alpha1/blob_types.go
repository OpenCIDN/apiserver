/*
Copyright 2025 The OpenCIDN Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// BlobKind is the kind of the Blob resource.
	BlobKind = "Blob"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient

// Blob is an API that describes the staged change of a resource
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Blob struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec holds information about the request being evaluated.
	Spec BlobSpec `json:"spec"`
	// Status holds status for the Blob
	Status BlobStatus `json:"status,omitempty"`
}

// BlobPhase defines the phase of a Blob
type BlobPhase string

const (
	// BlobPhasePending means the blob is pending
	BlobPhasePending BlobPhase = "Pending"
	// BlobPhaseRunning means the blob is running
	BlobPhaseRunning BlobPhase = "Running"
	// BlobPhaseSucceeded means the blob has succeeded
	BlobPhaseSucceeded BlobPhase = "Succeeded"
	// BlobPhaseFailed means the blob has failed
	BlobPhaseFailed BlobPhase = "Failed"
	// BlobPhaseUnknown means the blob status is unknown
	BlobPhaseUnknown BlobPhase = "Unknown"
)

// BlobStatus holds status for the Blob
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobStatus struct {
	// Phase represents the current phase of the blob.
	Phase BlobPhase `json:"phase,omitempty"`

	// Progress is the progress of the blob.
	Progress int64 `json:"progress,omitempty"`

	// Retry is the number of times the blob has been retried.
	Retry int64 `json:"retry,omitempty"`

	// Conditions holds conditions for the Blob.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// BlobSpec defines the specification for Blob.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobSpec struct {
	// HandlerName is the name of the handler.
	HandlerName string `json:"handlerName"`

	// Source is the source of the blob.
	Source string `json:"source"`

	// Destination is the destination of the blob.
	Destination []string `json:"destination"`

	// Weight represents the relative importance of this blob when multiple blobs exist.
	Weight int `json:"weight"`

	// Total represents the total amount of work to be done for this blob.
	Total int64 `json:"total"`

	// Hash is the hash of the blob content being verified.
	Hash string `json:"hash"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BlobList contains a list of Blob
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Blob `json:"items"`
}
