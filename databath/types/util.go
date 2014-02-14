package types

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"
)

func EscapeString(input string) string {
	return input
	// Handled by driver
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

func mapValueDefaultUInt64(m map[string]interface{}, key string, defaultVal uint64, reciever *uint64) error {
	interfaceVal, ok := m[key]
	if !ok {
		*reciever = defaultVal
	} else {
		v, err := getUnsignedInt(interfaceVal)
		if err != nil {
			return err
		}
		*reciever = v
	}
	return nil
}

func getUnsignedInt(input interface{}) (uint64, error) {
	switch input := input.(type) {
	case string:
		i, err := strconv.ParseUint(input, 10, 64)
		if err != nil {
			return 0, UserErrorF("Must be an unsigned integer, could not parse string '%s': %s", input, err.Error())
		}
		return getUnsignedInt(i)

	case uint64:
		return input, nil

	case uint32:
		return uint64(input), nil
	case int:
		if input < 0 {
			return 0, MakeToDbUserErrorFromString("Must be an unsigned integer (< 0 32)")
		}
		return uint64(input), nil
	case int32:
		if input < 0 {
			return 0, MakeToDbUserErrorFromString("Must be an unsigned integer (< 0 32)")
		}
		return uint64(input), nil
	case int64:
		if input < 0 {
			return 0, MakeToDbUserErrorFromString("Must be an unsigned integer (< 0 64)")
		}
		return uint64(input), nil
	case float64:

		if math.Mod(input, 1) != 0 {
			if input < 0 {
				return 0, MakeToDbUserErrorFromString("Must be an unsigned integer (float with decimal)")
			}
		}

		return getUnsignedInt(int64(math.Floor(input)))

	default:
		log.Printf("NOT INT: %v\n", input)
		return 0, makeConversionError("unsigned Int", input)
	}
}
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
			return "NULL", nil
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
