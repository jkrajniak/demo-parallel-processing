package processstate

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
)

type Getter struct {
	Service
}

func NewGetter(dbAPI dynamodbiface.DynamoDBAPI, tableName string) *Getter {
	return &Getter{NewService(dbAPI, tableName)}
}

func (getter *Getter) HasPendingProcesses(jobID string) (bool, error) {
	num, err := getter.countProcessesInState(jobID, StatePending)
	return num > 0, err
}

func (getter *Getter) countProcessesInState(jobID string, state ProcessState) (int64, error) {
	stateString := string(state)
	input := &dynamodb.QueryInput{
		TableName:              &getter.tableName,
		IndexName:              aws.String(stateProcessIDIndexName),
		KeyConditionExpression: aws.String("job_id = :jobId AND begins_with(state__process_id, :state)"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":jobId": {S: &jobID},
			":state": {S: &stateString},
		},
		Select: aws.String("COUNT"),
	}
	res, err := getter.api.Query(input)
	if err != nil {
		return 0, err
	}

	if res.Count == nil {
		return 0, errors.New("failed to get the count of the elements")
	}

	return *res.Count, nil
}
