package needy

import (
	"fmt"
	"reflect"
	"testing"
)

type I_Interface interface {
	DoThing() string
}

func TestTypeStrings(t *testing.T) {
	var v_interface I_Interface
	fmt.Printf("v_interface %%T: %T\n", v_interface)

	fmt.Printf("TypeOf(v_interface) %%T: %T\n", reflect.TypeOf(v_interface))

	fmt.Printf("ValueOf(v_interface).Kind().String(): %s\n", reflect.ValueOf(v_interface).Kind().String())
}
