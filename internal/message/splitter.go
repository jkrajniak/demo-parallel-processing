package message

type SplitterInput struct {
	JobID          string  `json:"job_id"`
	Bucket         string  `json:"bucket"`
	OutputBucket   string  `json:"output_bucket"`
	NextPageMarker *string `json:"next_page_marker"`
}
