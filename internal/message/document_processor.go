package message

type DocumentProcessorInput struct {
	SplitterInput
	ProcessID   string `json:"process_id"`
	DocumentKey string `json:"document_key"`
}
