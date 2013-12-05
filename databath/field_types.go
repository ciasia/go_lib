package databath

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"

	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

type Field interface {
	Init(map[string]interface{}) error
	FromDb(interface{}) (interface{}, error)
	ToDb(interface{}) (string, error)
	GetScanReciever() interface{}
	IsSearchable() bool
}

func EscapeString(input string) string {
	return fmt.Sprintf("\"%s\"", input)
}
func UnescapeString(input string) string {
	return input
}

type ModelDefinitionError struct {
	message   string
	fieldName string
}

func (err ModelDefinitionError) Error() string {
	return err.fieldName + ": " + err.message
}

func makeConversionError(expectedType string, input interface{}) error {
	gotType := reflect.TypeOf(input)
	if gotType == nil {
		return MakeToDbUserErrorFromString(fmt.Sprintf("Conversion error, Value Must be %s, got %s", expectedType, "null"))
	}
	return MakeToDbUserErrorFromString(fmt.Sprintf("Conversion error, Value Must be %s, got %s (%v)", expectedType, gotType.String(), input))
}

// string
type FieldString struct{}

func (f *FieldString) IsSearchable() bool { return true }

func (f *FieldString) Init(raw map[string]interface{}) error { return nil }

func (f *FieldString) FromDb(stored interface{}) (interface{}, error) {
	// String -> String

	storedStringPointer, ok := stored.(*string)
	if !ok {
		return nil, makeConversionError("string", stored)
	}

	if storedStringPointer == nil {
		return nil, nil
	} else {
		return UnescapeString(*storedStringPointer), nil
	}
}

func (f *FieldString) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString(fmt.Sprintf("Converting string to DB, Value Must be a string, got '%v'", input))
	}
	return EscapeString(inputString), nil
}
func (f *FieldString) GetScanReciever() interface{} {
	var s string
	var sp *string = &s
	return &sp
}

// id
type FieldId struct{}

func (f *FieldId) IsSearchable() bool { return false }

func (f *FieldId) Init(raw map[string]interface{}) error { return nil }

func (f *FieldId) FromDb(stored interface{}) (interface{}, error) {
	// uInt64 -> uInt64
	storedInt, ok := stored.(*uint64)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected uint64)")
	}
	if storedInt == nil {
		return nil, nil
	}
	return *storedInt, nil
}
func (f *FieldId) ToDb(input interface{}) (string, error) {
	// uInt64 -> uInt64
	return unsignedIntToDb(input)
}
func (f *FieldId) GetScanReciever() interface{} {
	var v uint64
	var vp *uint64 = &v
	return &vp
}

//ref
type FieldRef struct {
	Collection string
}

func (f *FieldRef) IsSearchable() bool { return false }

func (f *FieldRef) Init(raw map[string]interface{}) error {
	collection, ok := raw["collection"]
	if !ok {
		return ModelDefinitionError{"Field has no collection", ""}
	}
	collectionString := collection.(string)
	f.Collection = collectionString
	return nil
}

func (f *FieldRef) FromDb(stored interface{}) (interface{}, error) {
	// uInt64 -> Iunt64
	storedInt, ok := stored.(*uint64)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected uint64)")
	}
	if storedInt == nil {
		return nil, nil
	}
	return *storedInt, nil
}
func (f *FieldRef) ToDb(input interface{}) (string, error) {
	return unsignedIntToDb(input)
}
func (f *FieldRef) GetScanReciever() interface{} {
	var v uint64
	var vp *uint64 = &v
	return &vp
}

/////////
// int //
/////////
type FieldInt struct{}

func (f *FieldInt) IsSearchable() bool { return false }

func (f *FieldInt) Init(raw map[string]interface{}) error { return nil }

func (f *FieldInt) FromDb(stored interface{}) (interface{}, error) {
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
func (f *FieldInt) ToDb(input interface{}) (string, error) {
	// Int64 -> Int64
	switch input := input.(type) {
	case string:
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
func (f *FieldInt) GetScanReciever() interface{} {
	var v int64
	var vp *int64 = &v
	return &vp
}

//float
type FieldFloat struct{}

func (f *FieldFloat) IsSearchable() bool { return false }

func (f *FieldFloat) Init(raw map[string]interface{}) error { return nil }

func (f *FieldFloat) FromDb(stored interface{}) (interface{}, error) {
	// float64 -> float64

	storedFloatPointer, ok := stored.(*float64)
	if !ok {
		return nil, makeConversionError("float64", stored)
	}

	if storedFloatPointer == nil {
		return nil, nil
	} else {
		return *storedFloatPointer, nil
	}
}
func (f *FieldFloat) ToDb(input interface{}) (string, error) {
	// float64 -> float64

	inputInt, ok := input.(float64)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a float")
	}

	return fmt.Sprintf("%d", inputInt), nil
}

func (f *FieldFloat) GetScanReciever() interface{} {
	var v float64
	var vp *float64 = &v
	return &vp
}

//text
type FieldText struct{}

func (f *FieldText) IsSearchable() bool { return true }

func (f *FieldText) Init(raw map[string]interface{}) error { return nil }

func (f *FieldText) FromDb(stored interface{}) (interface{}, error) {
	// String -> String
	storedString, ok := stored.(*string)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected string)")
	}
	if storedString == nil {
		return nil, nil
	}
	return UnescapeString(*storedString), nil
}
func (f *FieldText) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a string")
	}
	return EscapeString(inputString), nil
}
func (f *FieldText) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}

//text
type FieldPassword struct{}

func (f *FieldPassword) IsSearchable() bool { return false }

func (f *FieldPassword) Init(raw map[string]interface{}) error { return nil }

func (f *FieldPassword) FromDb(stored interface{}) (interface{}, error) {
	// String -> String
	return "*******", nil
}
func (f *FieldPassword) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a string")
	}

	return "\"" + HashPassword(inputString) + "\"", nil
}
func (f *FieldPassword) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}

// string
type FieldFile struct{}

func (f *FieldFile) IsSearchable() bool { return true }

func (f *FieldFile) Init(raw map[string]interface{}) error { return nil }

func (f *FieldFile) FromDb(stored interface{}) (interface{}, error) {
	// String -> String

	storedStringPointer, ok := stored.(*string)
	if !ok {
		return nil, makeConversionError("string", stored)
	}

	if storedStringPointer == nil {
		return nil, nil
	} else {
		return UnescapeString(*storedStringPointer), nil
	}
}

func (f *FieldFile) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString(fmt.Sprintf("Converting string to DB, Value Must be a string, got '%v'", input))
	}
	return EscapeString(inputString), nil
}
func (f *FieldFile) GetScanReciever() interface{} {
	var s string
	var sp *string = &s
	return &sp
}

//date
type FieldDate struct{}

func (f *FieldDate) IsSearchable() bool { return false }

func (f *FieldDate) Init(raw map[string]interface{}) error { return nil }

func (f *FieldDate) FromDb(stored interface{}) (interface{}, error) {
	//

	storedString, ok := stored.(*string)
	if !ok {
		return nil, makeConversionError("date", stored)
	}

	if storedString == nil {
		return nil, nil
	}
	return *storedString, nil

}

func (f *FieldDate) ToDb(input interface{}) (string, error) {

	str, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a string")
	}

	return fmt.Sprintf("\"%s\"", str), nil
}

func (f *FieldDate) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}

//address
//array
//datetime
//enum
//auto_timestamp

func unsignedIntToDb(input interface{}) (string, error) {
	// uInt64 -> uInt64
	switch input := input.(type) {
	case string:
		i, err := strconv.ParseUint(input, 10, 64)
		if err != nil {
			return "", UserErrorF("Must be an unsigned integer, could not parse string '%s': %s", input, err.Error())
		}
		return unsignedIntToDb(i)

	case uint64:
		return fmt.Sprintf("%d", input), nil

	case uint32:
		return fmt.Sprintf("%d", input), nil
	case int:
		if input < 0 {
			return "", MakeToDbUserErrorFromString("Must be an unsigned integer (< 0 32)")
		}
		return fmt.Sprintf("%d", input), nil
	case int32:
		if input < 0 {
			return "", MakeToDbUserErrorFromString("Must be an unsigned integer (< 0 32)")
		}
		return fmt.Sprintf("%d", input), nil
	case int64:
		if input < 0 {
			return "", MakeToDbUserErrorFromString("Must be an unsigned integer (< 0 64)")
		}
		return fmt.Sprintf("%d", input), nil
	case float64:

		if math.Mod(input, 1) != 0 {
			if input < 0 {
				return "", MakeToDbUserErrorFromString("Must be an unsigned integer (float with decimal)")
			}
		}

		return unsignedIntToDb(int64(math.Floor(input)))

	default:
		if input == nil {
			return "", nil
		}
		log.Printf("NOT INT: %v\n", input)
		return "", makeConversionError("unsigned Int", input)
	}
}

func HashPassword(plaintext string) string {
	// Create the Salt: 256 random bytes
	saltBytes := make([]byte, 256, 256)
	_, _ = rand.Reader.Read(saltBytes)

	// Create a hasher
	hasher := sha256.New()

	// Append plaintext bytes
	hasher.Write([]byte(plaintext))

	// Append salt bytes
	hasher.Write(saltBytes)

	// Get the hash from the hasher
	hashBytes := hasher.Sum(nil)

	// [256 bytes of salt] + [x bytes of hash] to a base64 string to store salt and password in one field
	return base64.URLEncoding.EncodeToString(append(saltBytes, hashBytes...))
}
