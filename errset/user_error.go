package errset

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
)

type ErrorSet struct {
	errors     []error
	userErrors []string
}

func (s *ErrorSet) Add(err error) {
	if err != nil {
		s.errors = append(s.errors, err)
		log.Println(err)
	}
}

func (s *ErrorSet) AddUserIf(err error, user string) {
	if err != nil {
		s.errors = append(s.errors, err)
		log.Println(err)
		s.userErrors = append(s.userErrors, user)
	}
}

func (s *ErrorSet) AddUser(user string) {
	s.userErrors = append(s.userErrors, user)
}

func (s *ErrorSet) AddDirect(err error) {
	if err != nil {
		s.userErrors = append(s.userErrors, err.Error())
		s.errors = append(s.errors, err)
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

func NewSet() *ErrorSet {
	e := ErrorSet{make([]error, 0, 0), make([]string, 0, 0)}
	return &e
}
