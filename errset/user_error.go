package errset

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

type ErrorSet struct {
	errors     []error
	userErrors []string
	parentSet  *ErrorSet
	childSets  []*ErrorSet
}

func (s *ErrorSet) Error() string {
	return "There were errors in the request"
}

func (s *ErrorSet) GetHTTPStatus() int {
	return http.StatusBadRequest
}

func (s *ErrorSet) GetUserObject() interface{} {
	return s.GetErrors()
}

func (s *ErrorSet) parentAppendError(err error) {
	if s.parentSet != nil {
		s.parentSet.AddDirect(err)
	}
}
func (s *ErrorSet) parentAppendUser(err string) {
	if s.parentSet != nil {
		s.parentSet.AddUser(err)
	}

}
func (s *ErrorSet) Add(err error) {
	if err != nil {
		s.errors = append(s.errors, err)
		s.parentAppendError(err)
		log.Println(err)
	}
}

func (s *ErrorSet) AddUserIf(err error, user string) {
	if err != nil {
		s.errors = append(s.errors, err)
		s.parentAppendError(err)
		log.Println(err)
		s.userErrors = append(s.userErrors, user)
		s.parentAppendUser(user)
	}
}

func (s *ErrorSet) AddUser(user string) {
	s.userErrors = append(s.userErrors, user)
	s.parentAppendUser(user)
}

func (s *ErrorSet) AddUserf(format string, params ...interface{}) {
	user := fmt.Sprintf(format, params...)
	s.userErrors = append(s.userErrors, user)
	s.parentAppendUser(user)
}

func (s *ErrorSet) AddDirect(err error) {
	if err != nil {
		s.userErrors = append(s.userErrors, err.Error())
		s.parentAppendUser(err.Error())
		s.errors = append(s.errors, err)
		s.parentAppendError(err)
	}
}

func (s *ErrorSet) HasErrors() bool {
	if len(s.errors) > 0 || len(s.userErrors) > 0 {
		return true
	}
	return false
}

func (s *ErrorSet) GetErrors() []string {
	if len(s.userErrors) < 1 && len(s.errors) > 0 {
		s.userErrors = append(s.userErrors, "An unknown error occurred")
	}
	return s.userErrors
}

func (s *ErrorSet) PrintAll() {
	fmt.Printf("==========\nERROR DUMP\n----------\n")
	for _, err := range s.userErrors {
		fmt.Println(err)
	}
	fmt.Printf("--Non User Errors--\n")
	for _, err := range s.errors {
		fmt.Println(err)
	}
	fmt.Printf("----------\nEND DUMP\n==========\n")
}

func (e *ErrorSet) WriteJsonErrorResponse(w io.Writer) {
	respMap := make(map[string][]string)
	respMap["errors"] = e.GetErrors()
	b, _ := json.Marshal(respMap)
	fmt.Fprintf(w, "%s", b)
}

func (e *ErrorSet) GetChildSet() *ErrorSet {
	child := ErrorSet{
		errors:     make([]error, 0, 0),
		userErrors: make([]string, 0, 0),
		parentSet:  e,
		childSets:  make([]*ErrorSet, 0, 0),
	}
	return &child
}
func NewSet() *ErrorSet {
	e := ErrorSet{
		errors:     make([]error, 0, 0),
		userErrors: make([]string, 0, 0),
		parentSet:  nil,
		childSets:  make([]*ErrorSet, 0, 0),
	}
	return &e
}
