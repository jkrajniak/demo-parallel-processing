package splitter

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"
	"github.com/jkrajniak/demo-parallel-processing/internal/message"
	"github.com/nordcloud/ncerrors/errors"
	"github.com/sirupsen/logrus"
)

type processInitializer interface {
	Initialize(jobID, processID string) error
}

type outputToProcessor interface {
	Send(message message.DocumentProcessorInput) error
}

type internalOutput interface {
	Send(message message.SplitterInput) error
}

type observerOutput interface {
	Send(message message.ObserverInput) error
}

type LambdaHandler struct {
	outputToProcessor  outputToProcessor
	internalOutput     internalOutput
	processInitializer processInitializer
	observerOutput     observerOutput
	s3api              s3iface.S3API
}

func NewLambdaHandler(outputToProcessor outputToProcessor, internalOutput internalOutput, observerOutput observerOutput, api s3iface.S3API, initializer processInitializer) *LambdaHandler {
	return &LambdaHandler{outputToProcessor: outputToProcessor, internalOutput: internalOutput, observerOutput: observerOutput, s3api: api, processInitializer: initializer}
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

func (handler *LambdaHandler) handle(req message.SplitterInput) error {
	// iterate over objects in S3 bucket
	input := &s3.ListObjectsV2Input{
		Bucket:            &req.Bucket,
		ContinuationToken: req.NextPageMarker,
	}

	var errListing error
	err := handler.s3api.ListObjectsV2Pages(input, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		// If there is more than one page with object, first send to splitter next page to list
		if input.ContinuationToken != nil {
			req.NextPageMarker = input.ContinuationToken
			if errListing = handler.internalOutput.Send(req); errListing != nil {
				return false
			}
		}

		// iterate over objects in bucket
		for _, obj := range output.Contents {
			if obj == nil {
				continue
			}
			processMessage, err := prepareDocumentProcessMessage(req, *obj.Key)
			if err != nil {
				logrus.WithField("document", obj).WithError(err).Error("unable to prepare document to process")
				continue
			}
			// schedule new job
			if errListing = handler.outputToProcessor.Send(processMessage); errListing != nil {
				return false
			}
			if errListing = handler.processInitializer.Initialize(req.JobID, processMessage.ProcessID); errListing != nil {
				return false
			}
		}

		return !lastPage
	})
	if errListing != nil {
		return errListing
	}

	// Initialize process observer
	if err := handler.observerOutput.Send(message.ObserverInput{JobID: req.JobID, Attempt: 0}); err != nil {
		return err
	}

	return err
}

func prepareDocumentProcessMessage(req message.SplitterInput, keyName string) (message.DocumentProcessorInput, error) {
	processID, err := uuid.NewRandom()
	if err != nil {
		return message.DocumentProcessorInput{}, err
	}
	return message.DocumentProcessorInput{
		SplitterInput: req,
		ProcessID:     processID.String(),
		DocumentKey:   keyName,
	}, nil
}

func (handler *LambdaHandler) readMessageFromRecord(record events.SQSMessage) (message message.SplitterInput, err error) {
	err = json.Unmarshal([]byte(record.Body), &message)
	return
}
