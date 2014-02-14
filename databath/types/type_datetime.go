package types

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

type FieldDateTime struct{}

func (f *FieldDateTime) GetMysqlDef() string { return "INT(11) NULL" }

func (f *FieldDateTime) IsSearchable() bool { return false }

func (f *FieldDateTime) Init(raw map[string]interface{}) error { return nil }

func (f *FieldDateTime) FromDb(stored interface{}) (interface{}, error) {
	// Int64 -> Int64
	storedInt, ok := stored.(*int64)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected int64)")
	}
	if storedInt == nil {
		return nil, nil
	}
	return *storedInt, nil
}
func (f *FieldDateTime) ToDb(input interface{}) (string, error) {
	// Int64 -> Int64
	switch input := input.(type) {
	case string:
		if strings.HasPrefix(input, "#now") {

			t := time.Now().Unix()
			return f.ToDb(t)
		}

		i, err := strconv.ParseUint(input, 10, 64)
		if err != nil {
			return "", UserErrorF("Must be an integer, could not parse string '%s': %s", input, err.Error())
		}
		return f.ToDb(i)

	case uint64, uint32, int, int32, int64:
		return fmt.Sprintf("%d", input), nil

	case float64:
		if math.Mod(input, 1) != 0 {
			if input < 0 {
				return "", MakeToDbUserErrorFromString("Must be an unsigned integer (float with decimal)")
			}
		}

		return f.ToDb(int64(math.Floor(input)))

	default:
		if input == nil {
			return "", nil
		}
		log.Printf("NOT INT: %v\n", input)
		return "", makeConversionError("unsigned Int", input)
	}

	inputInt, ok := input.(int64)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be an integer")
	}

	return fmt.Sprintf("%d", inputInt), nil
}
func (f *FieldDateTime) GetScanReciever() interface{} {
	var v int64
	var vp *int64 = &v
	return &vp
}
