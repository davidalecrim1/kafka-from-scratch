package message

import (
	"encoding/binary"
	"fmt"
)

type DefaultRequest struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

func NewDefaultMessageFromBytes(buf []byte) (*DefaultRequest, error) {
	if len(buf) < 12 {
		return nil, fmt.Errorf("invalid request, the size in bytes is '%d', but expected is '%d'", len(buf), 10)
	}

	request := &DefaultRequest{
		MessageSize:       int32(binary.BigEndian.Uint32(buf[0:4])),
		RequestAPIKey:     int16(binary.BigEndian.Uint16(buf[4:6])),
		RequestAPIVersion: int16(binary.BigEndian.Uint16(buf[6:8])),
		CorrelationID:     int32(binary.BigEndian.Uint32(buf[8:])),
	}

	return request, nil
}

func (d *DefaultRequest) ToBytes() ([]byte, error) {
	buf := make([]byte, 12)

	binary.BigEndian.PutUint32(buf[0:4], uint32(d.MessageSize))
	binary.BigEndian.PutUint16(buf[4:6], uint16(d.RequestAPIKey))
	binary.BigEndian.PutUint16(buf[6:8], uint16(d.RequestAPIVersion))
	binary.BigEndian.PutUint32(buf[8:], uint32(d.CorrelationID))

	return buf, nil
}

type ErrorResponseMessage struct {
	messageSize   int32
	CorrelationID int32
	ErrorCode     int16
}

func (e *ErrorResponseMessage) ToBytes() ([]byte, error) {
	buf := make([]byte, 10)

	binary.BigEndian.PutUint32(buf[0:4], uint32(6)) // Only CorrelationID + ErrorCode
	binary.BigEndian.PutUint32(buf[4:8], uint32(e.CorrelationID))
	binary.BigEndian.PutUint16(buf[8:], uint16(e.ErrorCode))

	return buf, nil
}

func NewErrorResponseMessage(correlationID int32, errorCode int16) *ErrorResponseMessage {
	return &ErrorResponseMessage{
		messageSize:   6, // Only CorrelationID + ErrorCode
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
	}
}

func NewErrorResponseMessageFromBuffer(buf []byte) (*ErrorResponseMessage, error) {
	if len(buf) < 10 {
		return nil, fmt.Errorf("invalid request, the size in bytes is '%d', but expected is '%d'", len(buf), 10)
	}

	request := &ErrorResponseMessage{
		messageSize:   int32(binary.BigEndian.Uint32(buf[0:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(buf[4:8])),
		ErrorCode:     int16(binary.BigEndian.Uint16(buf[8:])),
	}

	return request, nil
}

type APIVersionsResponse struct {
	MessageSize     int32
	CorrelationID   int32
	ErrorCode       int16
	NumberOfAPIKeys int32
	APIVersions     []APIVersion
}

type APIVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func NewAPIVersionsResponse(correlationID int32) (*APIVersionsResponse, error) {
	return &APIVersionsResponse{
		CorrelationID: correlationID,
		ErrorCode:     int16(NoError),
		APIVersions: []APIVersion{
			{APIKey: int16(GetAPIVersions), MinVersion: 0, MaxVersion: 4},          // APIVersions API
			{APIKey: int16(DescribeTopicPartitions), MinVersion: 0, MaxVersion: 0}, // DescribeTopicPartitions API
		},
	}, nil
}

func (e *APIVersionsResponse) ToBytes() ([]byte, error) {
	apiVersionsSize := len(e.APIVersions) * 6 // Each entry is 6 bytes (APIKey + MinVersion + MaxVersion)
	totalSize := 10 + 4 + apiVersionsSize     // Fixed header (10 bytes) + NumAPIKeys (4 bytes) + API versions

	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint32(buf[0:4], uint32(totalSize-4)) // MessageSize (excluding itself)
	binary.BigEndian.PutUint32(buf[4:8], uint32(e.CorrelationID))
	binary.BigEndian.PutUint16(buf[8:10], uint16(e.ErrorCode))
	binary.BigEndian.PutUint32(buf[10:14], uint32(len(e.APIVersions))) // NumberOfAPIKeys

	offset := 14
	for _, api := range e.APIVersions {
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(api.APIKey))
		binary.BigEndian.PutUint16(buf[offset+2:offset+4], uint16(api.MinVersion))
		binary.BigEndian.PutUint16(buf[offset+4:offset+6], uint16(api.MaxVersion))
		offset += 6
	}

	return buf, nil
}

func NewAPIVersionsResponseFromBytes(buf []byte) (*APIVersionsResponse, error) {
	if len(buf) < 14 {
		return nil, fmt.Errorf("invalid response, expected at least 14 bytes but got '%d'", len(buf))
	}

	response := &APIVersionsResponse{
		CorrelationID: int32(binary.BigEndian.Uint32(buf[4:8])),
		ErrorCode:     int16(binary.BigEndian.Uint16(buf[8:10])),
	}

	numAPIKeys := int(binary.BigEndian.Uint32(buf[10:14]))
	offset := 14

	if len(buf) < offset+numAPIKeys*6 {
		return nil, fmt.Errorf("invalid response, expected at least '%d' bytes but got '%d'", offset+numAPIKeys*6, len(buf))
	}

	response.APIVersions = make([]APIVersion, numAPIKeys)
	for i := 0; i < numAPIKeys; i++ {
		apiKey := int16(binary.BigEndian.Uint16(buf[offset : offset+2]))
		minVersion := int16(binary.BigEndian.Uint16(buf[offset+2 : offset+4]))
		maxVersion := int16(binary.BigEndian.Uint16(buf[offset+4 : offset+6]))
		response.APIVersions[i] = APIVersion{APIKey: apiKey, MinVersion: minVersion, MaxVersion: maxVersion}
		offset += 6
	}

	return response, nil
}
