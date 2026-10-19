package rabbitmq

import (
	"bytes"
	"errors"
	"strconv"
)

// encodeIDs is the message body: decimal IDs joined by commas. One ID is the body the queue
// carried before batching, so older messages decode unchanged.
func encodeIDs(ids []int64) []byte {
	body := make([]byte, 0, len(ids)*8)
	for i, id := range ids {
		if i > 0 {
			body = append(body, ',')
		}
		body = strconv.AppendInt(body, id, 10)
	}
	return body
}

// decodeIDs parses a body made by encodeIDs. An empty body or a bad token is an error.
func decodeIDs(body []byte) ([]int64, error) {
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	ids := make([]int64, 0, bytes.Count(body, []byte{','})+1)
	for tok := range bytes.SplitSeq(body, []byte{','}) {
		id, err := strconv.ParseInt(string(tok), 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
