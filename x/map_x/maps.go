package map_x

import (
	"errors"
	"fmt"
	log "github.com/shyyawn/go-to/x/logging"
	"reflect"
	"strings"
)

func IntMap(in interface{}) (m map[string]interface{}) {
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Map {
		m = make(map[string]interface{})
		for _, key := range v.MapKeys() {
			val := v.MapIndex(key)
			m[fmt.Sprintf("%s", key.Interface())] = val.Interface()
		}
	}
	return m
}

// ToMap scans struct to create map[string]interface{}
// Deprecated: now use map https://github.com/mitchellh/mapstructure instead
func ToMap(data interface{}) (dataMap map[string]interface{}) {

	dataMap = make(map[string]interface{})

	// use reflection to get the values
	values := reflect.ValueOf(data)
	// if its a pointer, get the underlying element
	if values.Kind() == reflect.Ptr {
		if values.Elem().Kind() != reflect.Struct {
			return dataMap
		}
		values = values.Elem()
	}
	// use reflection to get the type
	types := values.Type()

	// number of fields greater than zero
	if values.NumField() > 0 {
		// read values and create object array
		for i := 0; i < values.NumField(); i++ {

			// field and type field
			field := values.Field(i)
			typeField := types.Field(i)

			// if can interface
			if field.CanInterface() {
				fieldType := typeField.Type.String() // field type
				// if is a native type
				fieldName := typeField.Tag.Get("json")
				if fieldName == "" {
					fieldName = typeField.Name
				}
				if !strings.Contains(fieldType, ".") {
					dataMap[fieldName] = values.Field(i).String()
				}
			}
		}
	}
	// if length of fields is zero, add warning for now
	if len(dataMap) == 0 {
		log.Warn("The schema is empty")
	}
	return dataMap
}

// MapTo scans map[string]interface{} to set struct values
// Deprecated: now use map https://github.com/mitchellh/mapstructure instead
func MapTo(m map[string]interface{}, obj interface{}) error {
	for name, value := range m {
		if err := SetField(obj, name, value); err != nil {
			return err
		}
	}
	return nil
}

// SetField scans map[string]interface{} to set struct values
// Deprecated: now use map https://github.com/mitchellh/mapstructure instead
func SetField(obj interface{}, name string, value interface{}) error {

	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("no such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.New("provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}
