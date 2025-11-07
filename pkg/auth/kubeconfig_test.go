package auth_test

import (
	"crypto/rsa"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/l7mp/dcontroller/pkg/auth"
)

var _ = Describe("Kubeconfig Generation", func() {
	var (
		privateKey *rsa.PrivateKey
		publicKey  *rsa.PublicKey
		tokenGen   *auth.TokenGenerator
	)

	BeforeEach(func() {
		privateKey, publicKey = mustGenerateKeyPair()
		Expect(privateKey).NotTo(BeNil())
		Expect(publicKey).NotTo(BeNil())

		tokenGen = auth.NewTokenGenerator(privateKey)
	})

	Describe("CreateKubeconfig", func() {
		It("should create a valid kubeconfig structure", func() {
			token, err := tokenGen.GenerateToken(
				"test-user",
				[]string{"default"},
				[]rbacv1.PolicyRule{
					{Verbs: []string{"get", "list"}, APIGroups: []string{"*"}, Resources: []string{"*"}},
				},
				time.Hour,
			)
			Expect(err).NotTo(HaveOccurred())

			config := auth.GenerateKubeconfig(
				"localhost:8443",
				"test-user",
				token,
				nil, // Use defaults
			)

			Expect(config).NotTo(BeNil())
			Expect(config.Clusters).To(HaveKey("dcontroller"))
			Expect(config.Clusters["dcontroller"].Server).To(Equal("https://localhost:8443"))
			Expect(config.Clusters["dcontroller"].InsecureSkipTLSVerify).To(BeFalse())

			Expect(config.AuthInfos).To(HaveKey("test-user"))
			Expect(config.AuthInfos["test-user"].Token).To(Equal(token))

			Expect(config.Contexts).To(HaveKey("dcontroller"))
			Expect(config.Contexts["dcontroller"].Cluster).To(Equal("dcontroller"))
			Expect(config.Contexts["dcontroller"].AuthInfo).To(Equal("test-user"))
			Expect(config.CurrentContext).To(Equal("dcontroller"))
		})

		It("should respect custom options", func() {
			token, err := tokenGen.GenerateToken("admin", []string{"*"}, nil, time.Hour)
			Expect(err).NotTo(HaveOccurred())

			opts := &auth.KubeconfigOptions{
				ClusterName:      "my-cluster",
				ContextName:      "my-context",
				DefaultNamespace: "kube-system",
				Insecure:         true,
				HTTPMode:         true,
			}

			config := auth.GenerateKubeconfig("api.example.com:6443", "admin", token, opts)

			Expect(config.Clusters).To(HaveKey("my-cluster"))
			Expect(config.Clusters["my-cluster"].InsecureSkipTLSVerify).To(BeTrue())
			Expect(config.Contexts).To(HaveKey("my-context"))
			Expect(config.Contexts["my-context"].Namespace).To(Equal("kube-system"))
			Expect(config.CurrentContext).To(Equal("my-context"))
			Expect(config.Clusters["my-cluster"].Server).To(Equal("http://api.example.com:6443"))
		})
	})

	Describe("Writing Kubeconfig to File", func() {
		It("should write a valid kubeconfig file", func() {
			token, err := tokenGen.GenerateToken("file-user", []string{"default"}, nil, time.Hour)
			Expect(err).NotTo(HaveOccurred())

			tempFile := "/tmp/test-kubeconfig-" + time.Now().Format("20060102150405")
			defer os.Remove(tempFile)

			// Create config structure
			config := auth.GenerateKubeconfig(
				"https://localhost:8443",
				"file-user",
				token,
				nil,
			)

			// Write to file
			err = clientcmd.WriteToFile(*config, tempFile)
			Expect(err).NotTo(HaveOccurred())

			// Verify file exists and can be read
			loadedConfig, err := clientcmd.LoadFromFile(tempFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(loadedConfig).NotTo(BeNil())
			Expect(loadedConfig.AuthInfos).To(HaveKey("file-user"))
			Expect(loadedConfig.AuthInfos["file-user"].Token).To(Equal(token))
		})
	})

	Describe("CreateRestConfig", func() {
		It("should create a valid rest.Config", func() {
			token, err := tokenGen.GenerateToken("rest-user", []string{"*"}, nil, time.Hour)
			Expect(err).NotTo(HaveOccurred())

			config := auth.CreateRestConfig("http://localhost:8080", token, true)

			Expect(config).NotTo(BeNil())
			Expect(config.Host).To(Equal("http://localhost:8080"))
			Expect(config.BearerToken).To(Equal(token))
			Expect(config.TLSClientConfig.Insecure).To(BeTrue())
		})

		It("should respect insecure flag", func() {
			config := auth.CreateRestConfig("https://secure.example.com", "token123", false)

			Expect(config.TLSClientConfig.Insecure).To(BeFalse())
		})
	})
})
