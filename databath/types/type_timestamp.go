package types

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

type FieldTimestamp struct {
	OnCreate bool
	OnUpdate bool
}

func (f *FieldTimestamp) GetMysqlDef() string {
	def := "TIMESTAMP"

	if f.OnCreate {
		def += " DEFAULT CURRENT_TIMESTAMP"
	}
	if f.OnUpdate {
		def += " ON UPDATE CURRENT_TIMESTAMP"
	}

	if def == "TIMESTAMP" {
		def = "TIMESTAMP NULL"
	}

	if def == "TIMESTAMP DEFAULT CURRENT_TIMESTAMP" {
		def = "TIMESTAMP NOT NULL"
	}
	return def
}

func (f *FieldTimestamp) IsSearchable() bool { return false }

func (f *FieldTimestamp) Init(raw map[string]interface{}) error {

	_, t := raw["onCreate"]
	if t {
		f.OnCreate = true
	}
	_, t = raw["onUpdate"]
	if t {
		f.OnUpdate = true
	}
	return nil
}

func (f *FieldTimestamp) FromDb(stored interface{}) (interface{}, error) {
	// Int64 -> Int64
	storedString, ok := stored.(*string)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected string)")
	}
	if storedString == nil {
		return nil, nil
	}
	if *storedString == "0000-00-00 00:00:00" {
		return 0, nil
	}
	d, err := time.Parse("2006-01-02 15:04:05", *storedString)
	if err != nil {
		return nil, err
	}
	i := d.Unix()
	return i, nil
}

func (f *FieldTimestamp) ToDb(input interface{}) (string, error) {
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

	case int64:
		i := int64(input)
		d := time.Unix(i, 0)

		return d.Format("2006-01-02 15:04:05"), nil

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
func (f *FieldTimestamp) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}
