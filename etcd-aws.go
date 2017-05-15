package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	// "path/filepath"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/crewjam/awsregion"
	"github.com/crewjam/ec2cluster"
	"golang.org/x/net/context"
)

type etcdState struct {
	Name       string         `json:"name"`
	ID         string         `json:"id"`
	State      string         `json:"state"`
	StartTime  time.Time      `json:"startTime"`
	LeaderInfo etcdLeaderInfo `json:"leaderInfo"`
}

type etcdLeaderInfo struct {
	Leader               string    `json:"leader"`
	Uptime               string    `json:"uptime"`
	StartTime            time.Time `json:"startTime"`
	RecvAppendRequestCnt int       `json:"recvAppendRequestCnt"`
	RecvPkgRate          int       `json:"recvPkgRate"`
	RecvBandwidthRate    int       `json:"recvBandwidthRate"`
	SendAppendRequestCnt int       `json:"sendAppendRequestCnt"`
}

type etcdMembers struct {
	Members []etcdMember `json:"members,omitempty"`
}

type etcdMember struct {
	ID         string   `json:"id,omitempty"`
	Name       string   `json:"name,omitempty"`
	PeerURLs   []string `json:"peerURLs,omitempty"`
	ClientURLs []string `json:"clientURLs,omitempty"`
}

type etcdHealth struct {
	Health string `json:"health"`
}

var localInstance *ec2.Instance
var peerProtocol string
var clientProtocol string
var etcdMajorVersion *string
var etcdCertFile *string
var etcdKeyFile *string
var etcdTrustedCaFile *string
var etcdClientPort *string
var etcdPeerPort *string
var clientTlsEnabled bool

func getHttpClient() (*http.Client, error) {
	var transport *http.Transport
	if clientTlsEnabled {
		cert, err := tls.LoadX509KeyPair(*etcdCertFile, *etcdKeyFile)
		if err != nil {
			return nil, fmt.Errorf("ERROR: %s", err)
		}
		caCert, err := ioutil.ReadFile(*etcdTrustedCaFile)
		if err != nil {
			return nil, fmt.Errorf("ERROR: %s", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
		transport = &http.Transport{
			TLSClientConfig:     tlsConfig,
			TLSHandshakeTimeout: 2 * time.Second,
		}
	} else {
		transport = &http.Transport{}
	}

	transport.IdleConnTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 2 * time.Second
	transport.ExpectContinueTimeout = 10 * time.Second
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second}
	return client, nil
}

func getEtcdClient(endpoints []string) (*clientv3.Client, error) {
	var etcdClient *clientv3.Client
	var err error
	if clientTlsEnabled {
		tlsInfo := transport.TLSInfo{
			CertFile:      *etcdCertFile,
			KeyFile:       *etcdKeyFile,
			TrustedCAFile: *etcdTrustedCaFile,
		}
		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("ERROR: %s", err)
		}
		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints: endpoints,
			TLS:       tlsConfig,
		})
		if err != nil {
			return nil, fmt.Errorf("ERROR: %s", err)
		}
	} else {
		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints: endpoints,
		})
		if err != nil {
			return nil, fmt.Errorf("ERROR: %s", err)
		}
	}
	return etcdClient, nil
}

func getApiResponse(privateIpAddress string, instanceId string, path string, method string) (*http.Response, error) {
	return getApiResponseWithBody(privateIpAddress, instanceId, path, method, "", nil)
}

func getApiResponseWithBody(privateIpAddress string, instanceId string, path string, method string, bodyType string, body io.Reader) (*http.Response, error) {
	var resp *http.Response
	var err error
	var req *http.Request

	if bodyType == "" {
		// health is an unversioned endpoint
		if path == "health" {
			req, _ = http.NewRequest(method, fmt.Sprintf("%s://%s:%s/%s",
				clientProtocol, privateIpAddress, *etcdClientPort, path), body)
		} else {
			req, _ = http.NewRequest(method, fmt.Sprintf("%s://%s:%s/v2/%s",
				clientProtocol, privateIpAddress, *etcdClientPort, path), body)
		}
	}

	client, err := getHttpClient()

	if bodyType != "" {
		client.Post(fmt.Sprintf("%s://%s:%s/v2/%s",
			clientProtocol, privateIpAddress, *etcdClientPort, path), bodyType, body)
	} else {
		resp, err = client.Do(req)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: %s %s://%s:%s/v2/%s: %s",
			instanceId, method, clientProtocol, privateIpAddress, *etcdClientPort, path, err)
	}
	return resp, nil
}

func buildCluster(s *ec2cluster.Cluster) (initialClusterState string, initialCluster []string, err error) {

	localInstance, err := s.Instance()
	if err != nil {
		return "", nil, err
	}

	clusterInstances, err := s.Members()
	if err != nil {
		return "", nil, fmt.Errorf("list members: %s", err)
	}

	initialClusterState = "new"
	initialCluster = []string{}
	for _, instance := range clusterInstances {
		if instance.PrivateIpAddress == nil {
			continue
		}
		log.Printf("getting stats from %s (%s)", *instance.InstanceId, *instance.PrivateIpAddress)

		// add this instance to the initialCluster expression
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s://%s:%s",
			*instance.InstanceId, peerProtocol, *instance.PrivateIpAddress, *etcdPeerPort))

		// skip the local node, since we know it is not running yet
		if *instance.InstanceId == *localInstance.InstanceId {
			continue
		}

		// fetch the state of the node.
		path := "stats/self"
		resp, err := getApiResponse(*instance.PrivateIpAddress, *instance.InstanceId, path, http.MethodGet)
		if err != nil {
			log.Printf("%s: %s://%s:%s/v2/%s: %s", *instance.InstanceId, clientProtocol,
				*instance.PrivateIpAddress, *etcdClientPort, path, err)
			continue
		}
		nodeState := etcdState{}
		if err := json.NewDecoder(resp.Body).Decode(&nodeState); err != nil {
			log.Printf("%s: %s://%s:%s/v2/%s: %s", *instance.InstanceId, clientProtocol,
				*instance.PrivateIpAddress, *etcdClientPort, path, err)
			continue
		}

		if nodeState.LeaderInfo.Leader == "" {
			log.Printf("%s: %s://%s:%s/v2/%s: alive, no leader", *instance.InstanceId, clientProtocol,
				*instance.PrivateIpAddress, *etcdClientPort, path)
			continue
		}

		log.Printf("%s: %s://%s:%s/v2/%s: has leader %s", *instance.InstanceId, clientProtocol,
			*instance.PrivateIpAddress, *etcdClientPort, path, nodeState.LeaderInfo.Leader)
		if initialClusterState != "existing" {
			initialClusterState = "existing"

			// inform the node we found about the new node we're about to add so that
			// when etcd starts we can avoid etcd thinking the cluster is out of sync.
			log.Printf("joining cluster via %s", *instance.InstanceId)
			m := etcdMember{
				Name: *localInstance.InstanceId,
				PeerURLs: []string{fmt.Sprintf("%s://%s:%s",
					peerProtocol, *localInstance.PrivateIpAddress, *etcdPeerPort)},
			}
			body, _ := json.Marshal(m)
			getApiResponseWithBody(*instance.PrivateIpAddress, *instance.InstanceId, "members", http.MethodPost, "application/json", bytes.NewReader(body))
		}
	}
	return initialClusterState, initialCluster, nil
}

func main() {
	instanceID := flag.String("instance", "",
		"The instance ID of the cluster member. If not supplied, then the instance ID is determined from EC2 metadata")
	clusterTagName := flag.String("tag", "aws:autoscaling:groupName",
		"The instance tag that is common to all members of the cluster")

	// defaultBackupInterval := 5 * time.Minute
	// if d := os.Getenv("ETCD_BACKUP_INTERVAL"); d != "" {
	// 	var err error
	// 	defaultBackupInterval, err = time.ParseDuration(d)
	// 	if err != nil {
	// 		log.Fatalf("ERROR: %s", err)
	// 	}
	// }
	// backupInterval := flag.Duration("backup-interval", defaultBackupInterval,
	// 	"How frequently to back up the etcd data to S3")
	// backupBucket := flag.String("backup-bucket", os.Getenv("ETCD_BACKUP_BUCKET"),
	// 	"The name of the S3 bucket where tha backup is stored. "+
	// 		"Environment variable: ETCD_BACKUP_BUCKET")
	// defaultBackupKey := "/etcd-backup.gz"
	// if d := os.Getenv("ETCD_BACKUP_KEY"); d != "" {
	// 	defaultBackupKey = d
	// }
	// backupKey := flag.String("backup-key", defaultBackupKey,
	// 	"The name of the S3 key where tha backup is stored. "+
	// 		"Environment variable: ETCD_BACKUP_KEY")

	defaultDataDir := "/var/lib/etcd"
	if d := os.Getenv("ETCD_DATA_DIR"); d != "" {
		defaultDataDir = d
	}
	dataDir := flag.String("data-dir", defaultDataDir,
		"The path to the etcd data directory. "+
			"Environment variable: ETCD_DATA_DIR")

	defaultLifecycleQueueName := ""
	if lq := os.Getenv("LIFECYCLE_QUEUE_NAME"); lq != "" {
		defaultLifecycleQueueName = lq
	}
	lifecycleQueueName := flag.String("lifecycle-queue-name", defaultLifecycleQueueName,
		"The name of the lifecycle SQS queue (optional). "+
			"Environment variable: LIFECYCLE_QUEUE_NAME")

	defaultEtcdMajorVersion := "2"
	if av := os.Getenv("ETCD_MAJOR_VERSION"); av != "" {
		defaultEtcdMajorVersion = av
	}
	etcdMajorVersion = flag.String("etcd-major-version", defaultEtcdMajorVersion,
		"Etcd API version (2, 3). "+
			"Environment variable: ETCD_MAJOR_VERSION")

	defaultClientPort := "2379"
	if cp := os.Getenv("ETCD_CLIENT_PORT"); cp != "" {
		defaultClientPort = cp
	}
	etcdClientPort = flag.String("etcd-client-port", defaultClientPort,
		"Etcd client port number. "+
			"Environment variable: ETCD_CLIENT_PORT")
	defaultPeerPort := "2380"
	if pp := os.Getenv("ETCD_PEER_PORT"); pp != "" {
		defaultPeerPort = pp
	}
	etcdPeerPort = flag.String("etcd-peer-port", defaultPeerPort,
		"Etcd peer port number. "+
			"Environment variable: ETCD_PEER_PORT")

	etcdCertFile = flag.String("etcd-cert-file", os.Getenv("ETCD_CERT_FILE"),
		"Path to the client server TLS cert file. "+
			"Environment variable: ETCD_CERT_FILE")
	etcdKeyFile = flag.String("etcd-key-file", os.Getenv("ETCD_KEY_FILE"),
		"Path to the client server TLS key file. "+
			"Environment variable: ETCD_KEY_FILE")
	etcdClientCertAuth := flag.String("etcd-client-cert-auth", os.Getenv("ETCD_CLIENT_CERT_AUTH"),
		"Enable client cert authentication. "+
			"Environment variable: ETCD_CLIENT_CERT_AUTH")
	etcdTrustedCaFile = flag.String("etcd-trusted-ca-file", os.Getenv("ETCD_TRUSTED_CA_FILE"),
		"Path to the client server TLS trusted CA key file. "+
			"Environment variable: ETCD_TRUSTED_CA_FILE")

	etcdPeerCertFile := flag.String("etcd-peer-cert-file", os.Getenv("ETCD_PEER_CERT_FILE"),
		"Path to the peer server TLS cert file. "+
			"Environment variable: ETCD_PEER_CERT_FILE")
	etcdPeerKeyFile := flag.String("etcd-peer-key-file", os.Getenv("ETCD_PEER_KEY_FILE"),
		"Path to the peer server TLS key file. "+
			"Environment variable: ETCD_PEER_KEY_FILE")
	etcdPeerClientCertAuth := flag.String("etcd-peer-client-cert-auth", os.Getenv("ETCD_PEER_CLIENT_CERT_AUTH"),
		"Enable peer client cert authentication. "+
			"Environment variable: ETCD_PEER_CLIENT_CERT_AUTH")
	etcdPeerTrustedCaFile := flag.String("etcd-peer-trusted-ca-file", os.Getenv("ETCD_PEER_TRUSTED_CA_FILE"),
		"Path to the peer client server TLS trusted CA key file. "+
			"Environment variable: ETCD_PEER_TRUSTED_CA_FILE")
	etcdHeartbeatInterval := flag.String("etcd-heartbeat-interval", os.Getenv("ETCD_HEARTBEAT_INTERVAL"),
		"Time (in milliseconds) of a heartbeat interval. "+
			"Environment variable: ETCD_HEARTBEAT_INTERVAL")
	etcdElectionTimeout := flag.String("etcd-election-timeout", os.Getenv("ETCD_ELECTION_TIMEOUT"),
		"Time (in milliseconds) for an election to timeout. "+
			"Environment variable: ETCD_ELECTION_TIMEOUT")

	flag.Parse()

	clientTlsEnabled = false
	clientProtocol = "http"
	if *etcdCertFile != "" {
		clientTlsEnabled = true
		clientProtocol = "https"
	}
	peerProtocol = "http"
	if *etcdPeerCertFile != "" {
		peerProtocol = "https"
	}

	var err error
	if *instanceID == "" {
		*instanceID, err = ec2cluster.DiscoverInstanceID()
		if err != nil {
			log.Fatalf("ERROR: %s", err)
		}
	}

	awsSession := session.New()
	if region := os.Getenv("AWS_REGION"); region != "" {
		awsSession.Config.WithRegion(region)
	}
	awsregion.GuessRegion(awsSession.Config)

	s := &ec2cluster.Cluster{
		AwsSession: awsSession,
		InstanceID: *instanceID,
		TagName:    *clusterTagName,
	}

	localInstance, err := s.Instance()
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	initialClusterState, initialCluster, err := buildCluster(s)
	log.Printf("initial cluster: %s %s", initialClusterState, initialCluster)

	// start the backup and restore goroutine.
	// shouldTryRestore := false
	// if initialClusterState == "new" {
	// 	_, err := os.Stat(filepath.Join(*dataDir, "member"))
	// 	if os.IsNotExist(err) {
	// 		shouldTryRestore = true
	// 	} else {
	// 		log.Printf("%s: %s", filepath.Join(*dataDir, "member"), err)
	// 	}
	// }
	go func() {
		// wait for etcd to start
		var etcdClient *clientv3.Client
		for {
			log.Printf("etcd connecting")
			etcdClient, err = getEtcdClient([]string{fmt.Sprintf("%s://%s:%s",
				clientProtocol, *localInstance.PrivateIpAddress, *etcdClientPort)})
			if err != nil {
				log.Fatalf("ERROR: %s", err)
			}
			defer etcdClient.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := etcdClient.Sync(ctx)
			cancel()
			if err != nil {
				log.Printf("waiting for etcd to start: %s", err)
			} else {
				log.Printf("etcd connected")
				resp, _ := etcdClient.MemberList(context.Background())
				log.Printf("etcd members: %s", resp.Members)
				break
			}
			time.Sleep(time.Second)
		}

		// if shouldTryRestore {
		// 	if err := restoreBackup(s, *backupBucket, *backupKey, *dataDir); err != nil {
		// 		log.Fatalf("ERROR: %s", err)
		// 	}
		// }

		// if err := backupService(s, *backupBucket, *backupKey, *dataDir, *backupInterval); err != nil {
		// 	log.Fatalf("ERROR: %s", err)
		// }
	}()

	// watch for lifecycle events and remove nodes from the cluster as they are
	// terminated.
	go watchLifecycleEvents(s, *lifecycleQueueName)

	// Run the etcd command
	cmd := exec.Command(fmt.Sprintf("etcd%s", *etcdMajorVersion))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = []string{
		fmt.Sprintf("ETCD_NAME=%s", *localInstance.InstanceId),
		fmt.Sprintf("ETCD_DATA_DIR=%s", *dataDir),
		fmt.Sprintf("ETCD_ADVERTISE_CLIENT_URLS=%s://%s:%s", clientProtocol, *localInstance.PrivateIpAddress, *etcdClientPort),
		fmt.Sprintf("ETCD_LISTEN_CLIENT_URLS=%s://0.0.0.0:%s", clientProtocol, *etcdClientPort),
		fmt.Sprintf("ETCD_LISTEN_PEER_URLS=%s://0.0.0.0:%s", peerProtocol, *etcdPeerPort),
		fmt.Sprintf("ETCD_INITIAL_CLUSTER_STATE=%s", initialClusterState),
		fmt.Sprintf("ETCD_INITIAL_CLUSTER=%s", strings.Join(initialCluster, ",")),
		fmt.Sprintf("ETCD_INITIAL_ADVERTISE_PEER_URLS=%s://%s:%s", peerProtocol, *localInstance.PrivateIpAddress, *etcdPeerPort),
		fmt.Sprintf("ETCD_CERT_FILE=%s", *etcdCertFile),
		fmt.Sprintf("ETCD_KEY_FILE=%s", *etcdKeyFile),
		fmt.Sprintf("ETCD_CLIENT_CERT_AUTH=%s", *etcdClientCertAuth),
		fmt.Sprintf("ETCD_TRUSTED_CA_FILE=%s", *etcdTrustedCaFile),
		fmt.Sprintf("ETCD_PEER_CERT_FILE=%s", *etcdPeerCertFile),
		fmt.Sprintf("ETCD_PEER_KEY_FILE=%s", *etcdPeerKeyFile),
		fmt.Sprintf("ETCD_PEER_CLIENT_CERT_AUTH=%s", *etcdPeerClientCertAuth),
		fmt.Sprintf("ETCD_PEER_TRUSTED_CA_FILE=%s", *etcdPeerTrustedCaFile),
		fmt.Sprintf("ETCD_HEARTBEAT_INTERVAL=%s", *etcdHeartbeatInterval),
		fmt.Sprintf("ETCD_ELECTION_TIMEOUT=%s", *etcdElectionTimeout),
	}
	asg, _ := s.AutoscalingGroup()
	if asg != nil {
		cmd.Env = append(cmd.Env, fmt.Sprintf("ETCD_INITIAL_CLUSTER_TOKEN=%s", *asg.AutoScalingGroupARN))
	}
	for _, env := range cmd.Env {
		log.Printf("%s", env)
	}
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s", err)
	}
}
