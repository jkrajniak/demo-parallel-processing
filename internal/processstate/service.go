package processstate

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type ProcessState string

const (
	StatePending ProcessState = "pending"
	StateDone    ProcessState = "done"
)

type JobState string

const (
	JobPending JobState = "pending"
	JobDone    JobState = "done"
	JobTimeout JobState = "timeout"
)

const (
	stateProcessIDIndexName = "stateProcessID"
)

type Process struct {
	JobID     string       `json:"job_id"`
	ProcessID string       `json:"process_id"`
	State     ProcessState `json:"state"`

	StateProcessIDIndex string `json:"state__process_id"`
}

func CreateProcessItem(jobID, processID string, state ProcessState) Process {
	return Process{
		JobID:               jobID,
		ProcessID:           processID,
		State:               state,
		StateProcessIDIndex: getStateProcessIDIndex(state, processID),
	}
}

type Service struct {
	api       dynamodbiface.DynamoDBAPI
	tableName string
}

func NewService(dbAPI dynamodbiface.DynamoDBAPI, tableName string) Service {
	return Service{dbAPI, tableName}
}

func getStateProcessIDIndex(state ProcessState, processID string) string {
	return fmt.Sprintf("%s__%s", state, processID)
}
