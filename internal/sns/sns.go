package sns

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
)

type MessageSender struct {
	API      snsiface.SNSAPI
	TopicARN string
}

func NewMessageSender(api snsiface.SNSAPI, topicARN string) *MessageSender {
	return &MessageSender{api, topicARN}
}

func (sender *MessageSender) SendMessage(message interface{}) error {
	messageBody, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = sender.API.Publish(&sns.PublishInput{
		Message:  aws.String(string(messageBody)),
		TopicArn: &sender.TopicARN,
	})
	return err
}
