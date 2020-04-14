package message

type ObserverInput struct {
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
}
