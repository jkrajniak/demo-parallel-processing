package observer

import (
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/jkrajniak/demo-parallel-processing/internal/message"
	"github.com/jkrajniak/demo-parallel-processing/internal/sns"
	"github.com/jkrajniak/demo-parallel-processing/internal/sqs"
)

type InternalOutput struct {
	sqs.MessageSender
}

func NewInternalOutput(sqsAPI sqsiface.SQSAPI, sqsURL string) *InternalOutput {
	return &InternalOutput{sqs.MessageSender{API: sqsAPI, QueueAddress: sqsURL}}
}

func (output *InternalOutput) Send(message message.ObserverInput) error {
	return output.SendMessage(message)
}

type NotifyOutput struct {
	sns.MessageSender
}

func NewNotifyOutput(snsAPI snsiface.SNSAPI, topicARN string) *NotifyOutput {
	return &NotifyOutput{sns.MessageSender{API: snsAPI, TopicARN: topicARN}}
}

func (notifyOutput *NotifyOutput) Send(message message.NotifyOutput) error {
	return notifyOutput.SendMessage(message)
}
