package observer

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jkrajniak/demo-parallel-processing/internal/message"
	"github.com/jkrajniak/demo-parallel-processing/internal/processstate"
	"github.com/nordcloud/ncerrors/errors"
	"github.com/sirupsen/logrus"
)

const maxAttempts = 120

type processGetter interface {
	HasPendingProcesses(jobID string) (bool, error)
}

type internalOutput interface {
	Send(message message.ObserverInput) error
}

type notifyOutput interface {
	Send(message message.NotifyOutput) error
}

type LambdaHandler struct {
	internalOutput internalOutput
	processGetter  processGetter
	notifyOutput   notifyOutput
	s3api          s3iface.S3API
}

func NewLambdaHandler(internalOutput internalOutput, notifyOutput notifyOutput, processGetter processGetter) *LambdaHandler {
	return &LambdaHandler{internalOutput: internalOutput, notifyOutput: notifyOutput, processGetter: processGetter}
}

func (handler *LambdaHandler) Handle(event events.SQSEvent) error {
	//process sqs messages
	for _, record := range event.Records {
		errFields := errors.Fields{"record": record}
		logrus.WithField("record", record).Debug("received message")
		message, err := handler.readMessageFromRecord(record)
		logrus.WithField("message", message).Debug("parsed message")
		if err != nil {
			errors.LogError(errors.WithContext(err, "failed to unmarshal message", errFields))
			continue
		}
		if err := handler.handle(message); err != nil {
			errors.LogError(errors.WithContext(err, "failed to process message", errFields))
			return err
		}
	}
	return nil
}

func (handler *LambdaHandler) handle(req message.ObserverInput) error {
	if req.Attempt > maxAttempts {
		return handler.notifyOutput.Send(message.NotifyOutput{JobID: req.JobID, Status: processstate.JobTimeout})
	}

	stillPending, err := handler.processGetter.HasPendingProcesses(req.JobID)

	if err != nil {
		return err
	}

	if stillPending {
		req.Attempt++
		return handler.internalOutput.Send(req)
	}

	return handler.notifyOutput.Send(message.NotifyOutput{JobID: req.JobID, Status: processstate.JobDone})
}

func (handler *LambdaHandler) readMessageFromRecord(record events.SQSMessage) (message message.ObserverInput, err error) {
	err = json.Unmarshal([]byte(record.Body), &message)
	return
}
