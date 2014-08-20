package extdb

import (
	"fmt"
	"reflect"
	"testing"
)

type TestDest struct {
	One string `sql:"one"`
	Two string `sql:"two"`
}

func TestPointerArray(t *testing.T) {
	return
	fmt.Println("START")

	tdb := DB{}

	dest := &TestDest{}

	dests, err := tdb.getPointerArray([]string{"one", "two"}, dest)
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
	}
	_, err = fmt.Sscanf("ValOne ValTwo", "%s %s", dests...)
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
	}

	if dest.One != "ValOne" {
		t.Fail()
	}
	if dest.Two != "ValTwo" {
		t.Fail()
	}

}

func TestSlice(t *testing.T) {

	var psl []*TestDest
	fmt.Println(psl)
	unpack(&psl)
	fmt.Println(psl)
}

func unpack(dest interface{}) {
	dVal := reflect.ValueOf(dest).Elem()
	t := reflect.TypeOf(dest).Elem().Elem().Elem()
	rowVal := reflect.New(t)
	var rowInterface interface{} := rowVal.Interface()
	
	dVal.Set(reflect.Append(dVal, rowVal))
}
