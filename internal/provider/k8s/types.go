package k8s

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// metaListOptions is the list options type accepted by the informer
// tweak function in k8s.go.
type metaListOptions = metav1.ListOptions

