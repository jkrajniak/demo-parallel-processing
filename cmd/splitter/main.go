package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jkrajniak/demo-parallel-processing/internal/env"
	"github.com/jkrajniak/demo-parallel-processing/internal/handler/splitter"
	"github.com/jkrajniak/demo-parallel-processing/internal/logging"
	"github.com/jkrajniak/demo-parallel-processing/internal/processstate"
	"github.com/sirupsen/logrus"

	lambdaLauncher "github.com/aws/aws-lambda-go/lambda"
)

const (
	regionEnvVarName   = "REGION"
	logLevelEnvVarName = "LOG_LEVEL"

	documentProcessorInputQueueURLEnvVarName = "DOCUMENT_PROCESSOR_INPUT_SQS_URL"
	internalInputQueueURLEnvVarName          = "INTERNAL_INPUT_SQS_URL"
	processStateTableNameEnvVarName          = "PROCESS_STATE_TABLE_NAME"
	observerInputQueueURLEnvVarName          = "OBSERVER_INPUT_SQS_URL"
)

func main() {
	logging.ConfigureLoggerFromEnv(logLevelEnvVarName)
	logrus.Debug("Cold start of splitter handler")

	region := env.LoadEnvVariableOrPanic(regionEnvVarName)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: &region},
	}))

	sqsAPI := sqs.New(sess)
	s3API := s3.New(sess)
	dbAPI := dynamodb.New(sess)

	outputToProcessor := splitter.NewOutputToDocumentProcessor(sqsAPI, env.LoadEnvVariableOrPanic(documentProcessorInputQueueURLEnvVarName))
	internalOutput := splitter.NewInternalOutput(sqsAPI, env.LoadEnvVariableOrPanic(internalInputQueueURLEnvVarName))
	observerOutput := splitter.NewObserverOutput(sqsAPI, env.LoadEnvVariableOrPanic(observerInputQueueURLEnvVarName))
	initializer := processstate.NewInitializer(dbAPI, env.LoadEnvVariableOrPanic(processStateTableNameEnvVarName))

	handler := splitter.NewLambdaHandler(outputToProcessor, internalOutput, observerOutput,  s3API, initializer)

	lambdaLauncher.Start(handler.Handle)
}
