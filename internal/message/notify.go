package message

import "github.com/jkrajniak/demo-parallel-processing/internal/processstate"

type NotifyOutput struct {
	JobID  string                `json:"job_id"`
	Status processstate.JobState `json:"status"`
}
