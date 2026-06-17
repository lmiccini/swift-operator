package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	rabbitmqv1 "github.com/openstack-k8s-operators/infra-operator/apis/rabbitmq/v1beta1"
	"github.com/openstack-k8s-operators/lib-common/modules/common/helper"
	swiftv1 "github.com/openstack-k8s-operators/swift-operator/api/v1beta1"
	"github.com/openstack-k8s-operators/swift-operator/internal/swiftproxy"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	testNamespace          = "test-namespace"
	testTransportSecret    = "rabbitmq-transport-secret"
	testNewTransportSecret = "rabbitmq-transport-secret-new"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = swiftv1.AddToScheme(s)
	_ = rabbitmqv1.AddToScheme(s)
	return s
}

func newTestHelper(t *testing.T, cl client.Client, s *runtime.Scheme, obj client.Object) *helper.Helper {
	t.Helper()
	h, err := helper.NewHelper(obj, cl, nil, s, logr.Discard())
	if err != nil {
		t.Fatalf("failed to create helper: %v", err)
	}
	return h
}

func getSecret(t *testing.T, cl client.Client, name, namespace string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	err := cl.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, secret)
	if err != nil {
		t.Fatalf("failed to get secret %s: %v", name, err)
	}
	return secret
}

func TestSwiftProxyTransportFinalizerAdd(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	transportSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTransportSecret,
			Namespace: testNamespace,
		},
	}
	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(transportSecret, instance).Build()
	h := newTestHelper(t, cl, s, instance)

	err := rabbitmqv1.ManageTransportSecretFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updated := getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updated, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "transport secret should have the consumer finalizer")
}

func TestSwiftProxyTransportFinalizerRemoveOnDelete(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	transportSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testTransportSecret,
			Namespace:  testNamespace,
			Finalizers: []string{swiftproxy.TransportConsumerFinalizer},
		},
	}
	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(transportSecret, instance).Build()
	h := newTestHelper(t, cl, s, instance)

	before := getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(before, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "finalizer should be present before removal")

	err := rabbitmqv1.RemoveTransportSecretConsumerFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updated := getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updated, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeFalse(), "transport secret should not have the consumer finalizer after removal")
}

func TestSwiftProxyTransportFinalizerRotation(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	oldSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTransportSecret,
			Namespace: testNamespace,
		},
	}
	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testNewTransportSecret,
			Namespace: testNamespace,
		},
	}
	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(oldSecret, newSecret, instance).Build()
	h := newTestHelper(t, cl, s, instance)

	// Add finalizer to old secret (simulating initial reconciliation)
	err := rabbitmqv1.ManageTransportSecretFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updatedOld := getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updatedOld, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "old secret should have the consumer finalizer")

	// Add finalizer to new secret (simulating rotation: new transport URL)
	err = rabbitmqv1.ManageTransportSecretFinalizer(
		ctx, h, testNamespace,
		testNewTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updatedNew := getSecret(t, cl, testNewTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updatedNew, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "new secret should have the consumer finalizer")

	// Old secret should still have the finalizer (deferred removal)
	updatedOld = getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updatedOld, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "old secret should still have the consumer finalizer before deferred cleanup")

	// Remove finalizer from old secret (simulating deferred cleanup after all sub-conditions are true)
	err = rabbitmqv1.RemoveTransportSecretConsumerFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updatedOld = getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updatedOld, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeFalse(), "old secret should lose the consumer finalizer after deferred cleanup")

	// New secret should still have its finalizer
	updatedNew = getSecret(t, cl, testNewTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updatedNew, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "new secret should still have the consumer finalizer")
}

func TestSwiftProxyTransportFinalizerIdempotent(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	transportSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testTransportSecret,
			Namespace:  testNamespace,
			Finalizers: []string{swiftproxy.TransportConsumerFinalizer},
		},
	}
	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(transportSecret, instance).Build()
	h := newTestHelper(t, cl, s, instance)

	err := rabbitmqv1.ManageTransportSecretFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updated := getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updated, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "finalizer should still be present")

	finalizerCount := 0
	for _, f := range updated.Finalizers {
		if f == swiftproxy.TransportConsumerFinalizer {
			finalizerCount++
		}
	}
	g.Expect(finalizerCount).To(gomega.Equal(1), "finalizer should not be duplicated")
}

func TestSwiftProxyTransportFinalizerRemoveEmptyName(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(instance).Build()
	h := newTestHelper(t, cl, s, instance)

	err := rabbitmqv1.RemoveTransportSecretConsumerFinalizer(
		ctx, h, testNamespace,
		"",
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "should be a no-op when secret name is empty")
}

func TestSwiftProxyTransportFinalizerRemoveNonExistentSecret(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(instance).Build()
	h := newTestHelper(t, cl, s, instance)

	err := rabbitmqv1.RemoveTransportSecretConsumerFinalizer(
		ctx, h, testNamespace,
		"nonexistent-secret",
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "should be a no-op when secret does not exist")
}

func TestSwiftProxyTransportFinalizerManageEmptySecretName(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(instance).Build()
	h := newTestHelper(t, cl, s, instance)

	err := rabbitmqv1.ManageTransportSecretFinalizer(
		ctx, h, testNamespace,
		"",
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "should be a no-op when secret name is empty")
}

func TestSwiftProxyTransportFinalizerPreservesOtherFinalizers(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTestScheme()
	ctx := context.Background()

	otherFinalizer := "some.other.org/finalizer"
	transportSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testTransportSecret,
			Namespace:  testNamespace,
			Finalizers: []string{otherFinalizer},
		},
	}
	instance := &swiftv1.SwiftProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "swiftproxy",
			Namespace: testNamespace,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(transportSecret, instance).Build()
	h := newTestHelper(t, cl, s, instance)

	err := rabbitmqv1.ManageTransportSecretFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updated := getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updated, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeTrue(), "consumer finalizer should be added")
	g.Expect(controllerutil.ContainsFinalizer(updated, otherFinalizer)).To(
		gomega.BeTrue(), "existing finalizer should be preserved")

	err = rabbitmqv1.RemoveTransportSecretConsumerFinalizer(
		ctx, h, testNamespace,
		testTransportSecret,
		swiftproxy.TransportConsumerFinalizer,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	updated = getSecret(t, cl, testTransportSecret, testNamespace)
	g.Expect(controllerutil.ContainsFinalizer(updated, swiftproxy.TransportConsumerFinalizer)).To(
		gomega.BeFalse(), "consumer finalizer should be removed")
	g.Expect(controllerutil.ContainsFinalizer(updated, otherFinalizer)).To(
		gomega.BeTrue(), "other finalizer should still be present")
}
