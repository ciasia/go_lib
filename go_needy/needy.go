package needy

import (
	"fmt"
)

type Needy struct {
	Injected map[string]interface{}
}

type NeedyError struct {
	Message string
}

func (ne *NeedyError) Error() string {
	return ne.Message
}
func errf(format string, parameters ...interface{}) *NeedyError {
	return &NeedyError{fmt.Sprintf(format, parameters...)}
}

func getTypeString(thing interface{}) string {
	// Should be a pointer to struct or interface...

	return fmt.Sprintf("%T %t", thing, thing)

	/*
		t := reflect.TypeOf(thing)
		if t == nil {
			return "NIL"
		}
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		return t.String()
	*/
}

func (n *Needy) Add(injectable interface{}) error {
	t := getTypeString(injectable)

	_, ok := n.Injected[t]
	if ok {
		return errf("Type %s already exists", t)
	}
	n.Injected[t] = injectable
	return nil
}

func (n *Needy) Get(injectable interface{}) error {
	t := getTypeString(injectable)

	_, ok := n.Injected[t]
	if ok {
		return errf("Type %s hasn't been set yet", t)
	}
	return nil
}
