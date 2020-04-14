package splitter

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/jkrajniak/demo-parallel-processing/internal/message"
	"github.com/jkrajniak/demo-parallel-processing/internal/sqs"
)

type OutputToDocumentProcessor struct {
	sqs.MessageSender
}

func NewOutputToDocumentProcessor(sqsAPI sqsiface.SQSAPI, sqsURL string) *OutputToDocumentProcessor {
	return &OutputToDocumentProcessor{sqs.MessageSender{API: sqsAPI, QueueAddress: sqsURL}}
}

func (output *OutputToDocumentProcessor) Send(message message.DocumentProcessorInput) error {
	return output.SendMessage(message)
}

type InternalOutput struct {
	sqs.MessageSender
}

func NewInternalOutput(sqsAPI sqsiface.SQSAPI, sqsURL string) *InternalOutput {
	return &InternalOutput{sqs.MessageSender{API: sqsAPI, QueueAddress: sqsURL}}
}

func (output *InternalOutput) Send(message message.SplitterInput) error {
	return output.SendMessage(message)
}

type ObserverOutput struct {
	sqs.MessageSender
}

func NewObserverOutput(sqsAPI sqsiface.SQSAPI, sqsURL string) *ObserverOutput {
	return &ObserverOutput{sqs.MessageSender{API: sqsAPI, QueueAddress: sqsURL}}
}

func (output *ObserverOutput) Send(message message.ObserverInput) error {
	return output.SendMessage(message)
}
