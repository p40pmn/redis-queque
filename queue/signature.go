package queue

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func genSignature(secret []byte, j *Job) (string, error) {
	h := hmac.New(sha256.New, secret)
	if _, err := h.Write([]byte(fmt.Sprintf("PROJECT:%s-ID:%s-QUEUE:%d", j.ProjectID, j.ID, j.QueueNumber))); err != nil {
		return "", fmt.Errorf("hmac.Write(): %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
