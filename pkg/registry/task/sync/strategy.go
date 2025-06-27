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

package sync

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"

	"github.com/OpenCIDN/apiserver/pkg/apis/task/v1alpha1"
)

// NewStrategy creates and returns a syncStrategy instance
func NewStrategy(typer runtime.ObjectTyper) syncStrategy {
	return syncStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Sync
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*v1alpha1.Sync)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Sync")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchSync is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
func MatchSync(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *v1alpha1.Sync) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

type syncStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

func (syncStrategy) NamespaceScoped() bool {
	return false
}

func (syncStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (syncStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

func (syncStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (syncStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

func (syncStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (syncStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (syncStrategy) Canonicalize(obj runtime.Object) {
}

func (syncStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate returns warnings for the given update.
func (syncStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
