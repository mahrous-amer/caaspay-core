package structs

import "fmt"

type PingRequest struct {
	Message string `json:"message"`
}

func (p *PingRequest) Validate() error {
	if p.Message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}

type PingResponse struct {
	Response string                 `json:"response"`
	Input    map[string]interface{} `json:"input"`
}

func (pr *PingResponse) Validate() error {
	if pr.Response == "" {
		return fmt.Errorf("response is required")
	}
	return nil
}
