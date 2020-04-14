package documentprocessor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jkrajniak/demo-parallel-processing/internal/message"
	"github.com/jkrajniak/demo-parallel-processing/internal/processstate"
	"github.com/nordcloud/ncerrors/errors"
	"github.com/sirupsen/logrus"
	"strings"
	"text/scanner"
)

type processSetter interface {
	SetState(jobID, processID string, state processstate.ProcessState) error
}

type LambdaHandler struct {
	processSetter processSetter
	api           s3iface.S3API
}

func NewLambdaHandler(setter processSetter, api s3iface.S3API) *LambdaHandler {
	return &LambdaHandler{processSetter: setter, api: api}
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

func (handler *LambdaHandler) handle(req message.DocumentProcessorInput) error {
	freq, err := handler.processDocument(req.Bucket, req.DocumentKey)
	if err != nil {
		return err
	}

	// put freq to output s3
	if err := handler.putFreq(req.OutputBucket, fmt.Sprintf("%s.freq", req.DocumentKey), freq); err != nil {
		return err
	}

	if err := handler.processSetter.SetState(req.JobID, req.ProcessID, processstate.StateDone); err != nil {
		return err
	}
	return nil
}

func (handler *LambdaHandler) putFreq(bucket, key string, freq map[string]int64) error {
	out, err := json.Marshal(freq)
	if err != nil {
		return err
	}

	_, err = handler.api.PutObject(&s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   aws.ReadSeekCloser(bytes.NewReader(out)),
	})
	return err
}

func (handler *LambdaHandler) processDocument(bucket, key string) (map[string]int64, error) {
	// get document from S3
	input := s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	out, err := handler.api.GetObject(&input)
	if err != nil {
		return nil, err
	}

	// Let use scanner to tokenize document.
	var scan scanner.Scanner
	scan.Init(out.Body)
	scan.Error = func(s *scanner.Scanner, msg string) {
		if msg == "invalid char literal" || msg == "literal not terminated" {
			return
		}
		
		pos := s.Position
		if !pos.IsValid() {
			pos = s.Pos()
		}
		logrus.Debug(fmt.Sprintf("scan error of %s/%s %s: %s\n", bucket, key, pos, msg))
	}
	freq := make(map[string]int64)
	for tok := scan.Scan(); tok != scanner.EOF; tok = scan.Scan() {
		toLower := strings.TrimSpace(strings.ToLower(scan.TokenText()))
		_, notIgnored := ignoreTokens[toLower]
		if isAlpha(toLower) && !notIgnored {
			freq[toLower]++
		}
	}
	return freq, nil
}

func (handler *LambdaHandler) readMessageFromRecord(record events.SQSMessage) (message message.DocumentProcessorInput, err error) {
	err = json.Unmarshal([]byte(record.Body), &message)
	return
}
