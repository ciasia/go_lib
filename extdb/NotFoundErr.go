package extdb

import ()

type NotFoundErr struct{}

func (e *NotFoundErr) Error() string {
	return "Not Found"
}
