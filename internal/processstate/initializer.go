package processstate

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Initializer struct {
	Service
}

func NewInitializer(dbAPI dynamodbiface.DynamoDBAPI, tableName string) *Initializer {
	return &Initializer{NewService(dbAPI, tableName)}
}

func (initializer *Initializer) Initialize(jobID, processID string) error {
	process := CreateProcessItem(jobID, processID, StatePending)

	item, err := dynamodbattribute.MarshalMap(process)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: &initializer.tableName,
		Item:      item,
	}

	_, err = initializer.api.PutItem(input)

	return err
}
