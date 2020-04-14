package sqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type MessageSender struct {
	API       sqsiface.SQSAPI
	QueueAddress string
}

func NewMessageSender(api sqsiface.SQSAPI, queueAddress string) *MessageSender {
	return &MessageSender{api, queueAddress}
}

func (sender *MessageSender) SendMessage(message interface{}) error {
	messageBody, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = sender.API.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(messageBody)),
		QueueUrl:    &sender.QueueAddress,
	})
	return err
}
