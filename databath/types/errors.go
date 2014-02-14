package types

import (
	"errors"
	"fmt"
)

type FromDbError struct {
	raw     error
	message *string
}

type ToDbUserError struct {
	message string
}

type ToDbInternalError struct {
	raw     error
	message *string
}

func (e *FromDbError) Error() string {
	if e.message != nil {
		return *e.message
	}
	return e.raw.Error()
}
func (e *ToDbInternalError) Error() string {
	return e.raw.Error()
}
func (e *ToDbUserError) Error() string {
	return e.message
}

func MakeFromDbErrorFromString(message string) *FromDbError {
	e := FromDbError{message: &message}
	return &e
}

func MakeToDbUserErrorFromString(message string) *ToDbUserError {
	e := ToDbUserError{message: message}
	return &e
}

func UserErrorF(format string, parameters ...interface{}) error {
	return errors.New(fmt.Sprintf(format, parameters...))
}
