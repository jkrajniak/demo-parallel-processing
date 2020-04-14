package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jkrajniak/demo-parallel-processing/internal/env"
	"github.com/jkrajniak/demo-parallel-processing/internal/handler/documentprocessor"
	"github.com/jkrajniak/demo-parallel-processing/internal/logging"
	"github.com/jkrajniak/demo-parallel-processing/internal/processstate"
	"github.com/sirupsen/logrus"

	lambdaLauncher "github.com/aws/aws-lambda-go/lambda"
)

const (
	regionEnvVarName   = "REGION"
	logLevelEnvVarName = "LOG_LEVEL"

	processStateTableNameEnvVarName = "PROCESS_STATE_TABLE_NAME"
)

func main() {
	logging.ConfigureLoggerFromEnv(logLevelEnvVarName)
	logrus.Debug("Cold start of document processor handler")

	region := env.LoadEnvVariableOrPanic(regionEnvVarName)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: &region},
	}))

	s3API := s3.New(sess)
	dbAPI := dynamodb.New(sess)

	setter := processstate.NewSetter(dbAPI, env.LoadEnvVariableOrPanic(processStateTableNameEnvVarName))

	handler := documentprocessor.NewLambdaHandler(setter, s3API)

	lambdaLauncher.Start(handler.Handle)
}
