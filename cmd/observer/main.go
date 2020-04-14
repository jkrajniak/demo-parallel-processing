package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jkrajniak/demo-parallel-processing/internal/env"
	"github.com/jkrajniak/demo-parallel-processing/internal/handler/observer"
	"github.com/jkrajniak/demo-parallel-processing/internal/logging"
	"github.com/jkrajniak/demo-parallel-processing/internal/processstate"
	"github.com/sirupsen/logrus"

	lambdaLauncher "github.com/aws/aws-lambda-go/lambda"
)

const (
	regionEnvVarName   = "REGION"
	logLevelEnvVarName = "LOG_LEVEL"

	internalInputQueueURLEnvVarName     = "INTERNAL_INPUT_SQS_URL"
	processStateTableNameEnvVarName     = "PROCESS_STATE_TABLE_NAME"
	jobStateNotifySNSTopicARNEnvVarName = "JOB_STATE_NOTIFY_SNS_TOPIC_ARN"
)

func main() {
	logging.ConfigureLoggerFromEnv(logLevelEnvVarName)
	logrus.Debug("Cold start of observer handler")

	region := env.LoadEnvVariableOrPanic(regionEnvVarName)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: &region},
	}))

	sqsAPI := sqs.New(sess)
	dbAPI := dynamodb.New(sess)

	internalOutput := observer.NewInternalOutput(sqsAPI, env.LoadEnvVariableOrPanic(internalInputQueueURLEnvVarName))
	notifyOutput := observer.NewNotifyOutput(sns.New(sess), env.LoadEnvVariableOrPanic(jobStateNotifySNSTopicARNEnvVarName))
	processGetter := processstate.NewGetter(dbAPI, env.LoadEnvVariableOrPanic(processStateTableNameEnvVarName))

	handler := observer.NewLambdaHandler(internalOutput, notifyOutput, processGetter)

	lambdaLauncher.Start(handler.Handle)
}
