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
	MessageSize   int32
	CorrelationID int32
	ErrorCode     int16
}

func (e *ErrorResponseMessage) ToBytes() ([]byte, error) {
	buf := make([]byte, 10)

	binary.BigEndian.PutUint32(buf[0:4], uint32(e.MessageSize))   // message_size
	binary.BigEndian.PutUint32(buf[4:8], uint32(e.CorrelationID)) // correlation_id
	binary.BigEndian.PutUint16(buf[8:], uint16(e.ErrorCode))      // error_code

	return buf, nil
}

func NewErrorResponseMessage(buf []byte) (*ErrorResponseMessage, error) {
	if len(buf) < 10 {
		return nil, fmt.Errorf("invalid request, the size in bytes is '%d', but expected is '%d'", len(buf), 10)
	}

	request := &ErrorResponseMessage{
		MessageSize:   int32(binary.BigEndian.Uint32(buf[0:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(buf[4:8])),
		ErrorCode:     int16(binary.BigEndian.Uint16(buf[8:])),
	}

	return request, nil
}
