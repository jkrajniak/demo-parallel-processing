package processstate

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Setter struct {
	Service
}

func NewSetter(dbAPI dynamodbiface.DynamoDBAPI, tableName string) *Setter {
	return &Setter{NewService(dbAPI, tableName)}
}

func (setter *Setter) SetState(jobID, processID string, state ProcessState) error {
	input := &dynamodb.UpdateItemInput{
		TableName: &setter.tableName,
		Key: map[string]*dynamodb.AttributeValue{
			"job_id":     {S: &jobID},
			"process_id": {S: &processID},
		},
		UpdateExpression: aws.String("SET #state = :state, state__process_id = :stateProcessID"),
		ExpressionAttributeNames: map[string]*string{
			"#state": aws.String("state"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":state":          {S: aws.String(string(state))},
			":stateProcessID": {S: aws.String(getStateProcessIDIndex(state, processID))},
		},
	}

	_, err := setter.api.UpdateItem(input)

	return err
}
