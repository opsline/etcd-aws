package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/crewjam/ec2cluster"
)

// handleLifecycleEvent is invoked whenever we get a lifecycle
// launching or terminating message. It notifies the ASG when launching
// instances may be brought into service. And removes terminating instances
// from the etcd cluster. Calls are retried as the cluster may be unable
// to add or remove members if doing so would cause the cluster to become
// unhealthy.
func handleLifecycleEvent(m *ec2cluster.LifecycleMessage) (shouldContinue bool, err error) {
	switch m.LifecycleTransition {
	default:
		return true, nil
	case "autoscaling:EC2_INSTANCE_LAUNCHING":
		// TODO: move this to ec2cluster?
		ec2Svc := ec2.New(awsSession)
		ec2Resp, err := ec2Svc.DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIds: []*string{aws.String(m.EC2InstanceID)},
		})

		if err != nil {
			return false, err
		}

		if len(ec2Resp.Reservations) != 1 || len(ec2Resp.Reservations[0].Instances) != 1 {
			return false, fmt.Errorf("Cannot find instance %s", m.EC2InstanceID)
		}

		instance := ec2Resp.Reservations[0].Instances[0]
		var health etcdHealth
		for i := 0; i < 10; i++ {
			resp, err := getApiResponse(*instance.PrivateIpAddress, *localInstance.InstanceId, "health", http.MethodGet)
			if err != nil {
				if i == 10 {
					log.Printf("health query failed; erroring")
					return false, err
				}

				log.Printf("health query failed; sleeping")
				time.Sleep(1 + time.Duration(i)*3*time.Second)
				continue
			}

			if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
				log.Printf("json decoding error")
				return false, err
			}

			if health.Health == "true" {
				break
			}

			if i == 10 {
				log.Printf("new node unable to become healthy: %s", m.EC2InstanceID)
				return false, fmt.Errorf("new node unable to become healthy: %s", m.EC2InstanceID)
			}
		}

		return true, nil
	case "autoscaling:EC2_INSTANCE_TERMINATING":
		// look for the instance in the cluster
		resp, err := getApiResponse(*localInstance.PrivateIpAddress, *localInstance.InstanceId, "members", http.MethodGet)
		if err != nil {
			return false, err
		}
		members := etcdMembers{}
		if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
			return false, err
		}
		memberID := ""
		for _, member := range members.Members {
			if member.Name == m.EC2InstanceID {
				memberID = member.ID
			}
		}

		if memberID == "" {
			log.WithField("InstanceID", m.EC2InstanceID).Warn("received termination event for non-member")
			return true, nil
		}

		log.WithFields(log.Fields{
			"InstanceID": m.EC2InstanceID,
			"MemberID":   memberID}).Info("removing from cluster")

		for i := 0; i < 10; i++ {
			resp, err = getApiResponse(*localInstance.PrivateIpAddress, *localInstance.InstanceId, fmt.Sprintf("members/%s", memberID), http.MethodDelete)
			if err != nil {
				if i == 10 {
					log.Printf("delete member failed; erroring")
					return false, err
				}

				log.Printf("delete member failed; sleeping")
				time.Sleep(1 + time.Duration(i)*3*time.Second)
				continue
			}

			if resp.StatusCode == http.StatusOK {
				break
			}

			if i == 10 {
				log.Printf("node unable to be removed from cluster: %s", m.EC2InstanceID)
				return false, fmt.Errorf("node unable to be removed from cluster: %s", m.EC2InstanceID)
			}
		}

		return false, nil
	}
}

func watchLifecycleEvents(s *ec2cluster.Cluster, queueName string) {
	localInstance, _ = s.Instance()
	for {
		q, err := LifecycleEventQueueURL(s, queueName)

		if err != nil {
			log.Fatalf("ERROR: LifecycleEventQueueURL: %s", err)
		}

		log.Printf("SQS queue URL: %s", q)
		err = s.WatchLifecycleEvents(q, handleLifecycleEvent)

		// The lifecycle hook might not exist yet if we're being created
		// by cloudformation.
		if err == ec2cluster.ErrLifecycleHookNotFound {
			log.Printf("WARNING: %s", err)
			time.Sleep(10 * time.Second)
			continue
		}
		if err != nil {
			log.Fatalf("ERROR: WatchLifecycleEvents: %s", err)
		}
		panic("not reached")
	}
}

func LifecycleEventQueueURL(s *ec2cluster.Cluster, queueName string) (string, error) {
	asg, err := s.AutoscalingGroup()
	if err != nil {
		return "", err
	}

	autoscalingSvc := autoscaling.New(s.AwsSession)
	resp, err := autoscalingSvc.DescribeLifecycleHooks(&autoscaling.DescribeLifecycleHooksInput{
		AutoScalingGroupName: asg.AutoScalingGroupName,
	})
	if err != nil {
		return "", err
	}

	sqsSvc := sqs.New(s.AwsSession)
	for _, hook := range resp.LifecycleHooks {
		if !strings.HasPrefix(*hook.NotificationTargetARN, "arn:aws:sqs:") {
			continue
		}
		arnParts := strings.Split(*hook.NotificationTargetARN, ":")
		qName := arnParts[len(arnParts)-1]
		qOwnerAWSAccountID := arnParts[len(arnParts)-2]

		if queueName != "" && !strings.Contains(qName, "-"+queueName+"-") {
			continue
		}

		resp, err := sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName:              &qName,
			QueueOwnerAWSAccountId: &qOwnerAWSAccountID,
		})
		if err != nil {
			return "", err
		}
		return *resp.QueueUrl, nil
	}
	return "", ec2cluster.ErrLifecycleHookNotFound
}
