package sarama_x

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	c "github.com/logrusorgru/aurora/v3"
	"github.com/mitchellh/mapstructure"
	"github.com/riferrei/srclient"
	log "github.com/shyyawn/go-to/x/logging"
)

type SchemaRegistryHost struct {
	RegistryHost            string
	AvroSchemaCache         map[string]AvroSchemaCacheObj
	AvroSchemaRegistryCache map[int]*srclient.Schema
}

func (host *SchemaRegistryHost) Init(schemaHost string) error {
	if schemaHost == "" {
		return fmt.Errorf("Host cannot be empty")
	}
	host.RegistryHost = schemaHost
	host.AvroSchemaCache = map[string]AvroSchemaCacheObj{}
	host.AvroSchemaRegistryCache = map[int]*srclient.Schema{}
	return nil
}

func (host *SchemaRegistryHost) GetAvroSchema(namespace string, name string, data interface{}) (schema AvroSchema) {
	return host.GenerateAvroSchema(namespace, name, data, nil)
}

// GenerateAvroSchema scans struct to create avro schema
func (host *SchemaRegistryHost) GenerateAvroSchema(namespace string, name string, data interface{}, values *reflect.Value) (schema AvroSchema) {

	// if cached return from schema
	if cachedSchema, ok := host.AvroSchemaCache[name]; ok {
		return cachedSchema.Schema
	}

	// set basic fields for schema
	schema.Namespace = namespace // subfields, do they need namespace?
	schema.Type = AvroTypeRecord
	// this is done or else anything before the dot (.) will replace as namespace
	schema.Name = strings.ReplaceAll(name, ".", "_")
	schema.Fields = []AvroSchemaField{}

	// use reflection to get the values
	if values == nil {
		val := reflect.ValueOf(data)

		// if its a pointer, get the underlying element
		if val.Kind() == reflect.Ptr {
			if val.Elem().Kind() != reflect.Struct {
				return schema
			}
			val = val.Elem()
		}
		values = &val
	}

	// number of fields greater than zero
	if values.NumField() > 0 {
		// read values and create object array
		host.createSchema(namespace, *values, &schema.Fields)
		// create json from the schema object, as will cache both schema object and json
		schemaJson, _ := json.Marshal(schema)

		// add to cache the schema
		host.AvroSchemaCache[name] = AvroSchemaCacheObj{
			Schema: schema,
			Json:   schemaJson,
		}
	}
	// if length of fields is zero, need to add empty cache object
	if len(schema.Fields) == 0 {
		log.Warn("The schema is empty", schema.Name, schema.Type)
		host.AvroSchemaCache[name] = AvroSchemaCacheObj{}
	}
	return schema
}

func (host *SchemaRegistryHost) createSchema(namespace string, values reflect.Value, schemaFields *[]AvroSchemaField) {

	// use reflection to get the type
	types := values.Type()

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
			if !typeField.Anonymous {
				// If this is a type, need to convert this to a record

				var inFieldType interface{}

				if strings.Contains(fieldType, ".") {
					host.GenerateAvroSchema(namespace, fieldType, nil, &field)
					inFieldType = host.GetAvroSchema(namespace, fieldType, field)
				} else {
					switch fieldType {
					case "int64":
						inFieldType = "long"
					case "map[string]interface {}":
						// this should pass as json string
						inFieldType = "string"
					default:
						// @todo: untested, go's types and avro types are bit different, need a conversion function
						inFieldType = fieldType
						if strings.Contains(fieldType, "map[string]") {
							// @todo: untested, go's types and avro types are bit different, need a conversion function
							valType := strings.ReplaceAll(strings.ReplaceAll(fieldType, "map[string]", ""),
								" {}", "")
							inFieldType = AvroSchemaMap{
								Type:   "map",
								Values: valType,
							}
						}
					}
				}
				log.Info(c.BrightYellow("Matched"), c.Cyan(typeField.Type), c.BgCyan(typeField.Name), c.BrightGreen(fieldName), c.BgBrightGreen(field.Kind()))
				*schemaFields = append(*schemaFields, AvroSchemaField{
					Name: fieldName,
					Type: inFieldType,
				})
			} else { // Flatten anonymous types
				host.createSchema(namespace, field, schemaFields)
			}
		}
	}
}

// GetAvroSchemaJson scans struct to create avro schema json []byte
func (host *SchemaRegistryHost) GetAvroSchemaJson(namespace string, name string, data interface{}) (schemaJson []byte) {
	// if cached, then return from cache
	if cachedSchema, ok := host.AvroSchemaCache[name]; ok {
		return cachedSchema.Json
	}
	// if not in cache, get schema object
	host.GetAvroSchema(namespace, name, data)
	schemaJson = host.AvroSchemaCache[name].Json
	return schemaJson
}

// EnsureAvroEncoded avro encodes the type
func (host *SchemaRegistryHost) EnsureAvroEncoded(namespace string, encoded []byte, err error, name string, encoder sarama.Encoder) ([]byte, error) {

	schema := host.GetAvroSchemaJson(namespace, name, encoder)

	data := make(map[string]interface{})
	mapConfig := &mapstructure.DecoderConfig{
		TagName: "json",
		Result:  &data,
		Squash:  true,
	}
	decoder, _ := mapstructure.NewDecoder(mapConfig)
	if err := decoder.Decode(encoder); err != nil {
		log.Fatal(err)
	}

	if encoded == nil && err == nil {
		codec, err := goavro.NewCodec(string(schema))
		if err != nil {
			log.Fatalf("Failed to create the Avro Codec: %v", err)
		}
		binaryValue, err := codec.BinaryFromNative(nil, data)
		if err != nil {
			log.Fatalf("Failed to convert Go map to Avro binary data: %v", err)
		}
		log.Info("Kafka Messaged:", string(binaryValue))

		var binaryMsg []byte
		binaryMsg = append(binaryMsg, byte(0))
		binarySchemaId := make([]byte, 4)
		binary.BigEndian.PutUint32(binarySchemaId, uint32(1))
		binaryMsg = append(binaryMsg, binarySchemaId...)
		binaryMsg = append(binaryMsg, binaryValue...)

		log.Info("Kafka Finaled:", string(binaryValue))
		encoded = binaryMsg
	}

	return encoded, err
}

func (host *SchemaRegistryHost) GetSchemaById(schemaId int) (*srclient.Schema, error) {

	// RegistryHost has to be passed in a better way then been monkey patched like this
	if host.RegistryHost == "" {
		return nil, ErrNoRegistryHostDefined
	}

	// if cached return from schema registry cache
	if cachedSchema, ok := host.AvroSchemaRegistryCache[schemaId]; ok {
		return cachedSchema, nil
	}

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(host.RegistryHost)

	latestSchema, err := schemaRegistryClient.GetSchema(schemaId)
	if err != nil {
		return nil, fmt.Errorf("%w - schema error occurred", err)
	}

	// Set in cache
	host.AvroSchemaRegistryCache[schemaId] = latestSchema

	// if no error, then return the schema
	return latestSchema, nil
}

func (host *SchemaRegistryHost) GetSchemaBySubject(subject string) (*srclient.Schema, error) {

	// RegistryHost has to be passed in a better way then been monkey patched like this
	if host.RegistryHost == "" {
		return nil, ErrNoRegistryHostDefined
	}

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(host.RegistryHost)

	latestSchema, err := schemaRegistryClient.GetLatestSchema(subject)
	if err != nil {
		if strings.Contains(err.Error(), "404 Not Found: Subject") ||
			strings.Contains(err.Error(), " not found.") {
			log.Error(err.Error())
			return nil, fmt.Errorf("%w - with the error: %s", ErrSubjectNotFound, err.Error())
		}
		return nil, fmt.Errorf("%w - unable to get schema", err)
	}

	return latestSchema, nil
}

func (host *SchemaRegistryHost) CreateSchemaForSubject(subject, namespace, name string, encoder sarama.Encoder) (*srclient.Schema, error) {
	log.Info("Trying to create subject %s in schema registry", subject)

	// RegistryHost has to be passed in a better way then been monkey patched like this
	if host.RegistryHost == "" {
		log.Error("CreateSchemaForSubject No Reg Host:", host.RegistryHost)
		return nil, ErrNoRegistryHostDefined
	}

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(host.RegistryHost)

	// Generate the schema from struct
	subjectSchema := host.GetAvroSchemaJson(namespace, name, encoder)

	log.Info(srclient.Avro, " | ", subject, " -> ", string(subjectSchema))
	schema, err := schemaRegistryClient.CreateSchema(subject, string(subjectSchema), srclient.Avro)
	if err != nil {
		log.Error("CreateSchemaForSubject Error:", err.Error())
		return nil, err
	}
	log.Info("CreateSchemaForSubject - Created:", schema.ID())
	return schema, nil
}

func (host *SchemaRegistryHost) MatchSchemaForSubject(subject, namespace, name string, existingSchema string, encoder sarama.Encoder) bool {
	log.Info("Match subject %s in schema registry", subject)
	// Generate the schema from struct
	newSubjectSchema := host.GetAvroSchemaJson(namespace, name, encoder)

	// @todo: Way too much unmarshalling
	existingAvroSubjectSchema := AvroSchema{}
	err := json.Unmarshal([]byte(existingSchema), &existingAvroSubjectSchema)
	if err != nil {
		log.Info(err.Error())
	}
	newAvroSubjectSchema := AvroSchema{}
	err = json.Unmarshal(newSubjectSchema, &newAvroSubjectSchema)
	if err != nil {
		log.Info(err.Error())
	}
	matched := true

	// Match schema Meta Fields
	if existingAvroSubjectSchema.Type != newAvroSubjectSchema.Type ||
		existingAvroSubjectSchema.Name != newAvroSubjectSchema.Name ||
		existingAvroSubjectSchema.Namespace != newAvroSubjectSchema.Namespace ||
		len(existingAvroSubjectSchema.Fields) != len(newAvroSubjectSchema.Fields) {
		log.Warn("Not Matching META in Schema")
		matched = false
	}

	if matched {
		// Start matching field types
		// 1. Create a map of existing fields
		existingFields := make(map[string]AvroSchemaField)
		for _, existingField := range existingAvroSubjectSchema.Fields {
			existingFields[existingField.Name] = existingField
		}
		// Loop new fields to check type
		for _, newField := range newAvroSubjectSchema.Fields {
			// Check if it exists in old one, and as the length is same of fields array, a miss means something changed
			existingField, ok := existingFields[newField.Name]
			if ok {
				// @todo: Should not use reflect, better implementation can be done here
				if reflect.TypeOf(existingField.Type).String() == "map[string]interface {}" &&
					reflect.TypeOf(newField.Type).String() == "map[string]interface {}" {

					// @todo: again more marshalling, this can be improved and the sequence of fields can cause false
					//          detection of field type not matching
					existingFieldJson, err := json.Marshal(existingField)
					if err != nil {
						// Fatal is not the best way to go, but since I haven't tested it extensively
						// a crash is better than debugging weird errors or rather weird code execution
						// so am forced to fix this when the needed arises.
						log.Fatal(err)
					}
					newFieldJson, err := json.Marshal(newField)
					if err != nil {
						// Fatal is not the best way to go, but since I haven't tested it extensively
						// a crash is better than debugging weird errors or rather weird code execution
						// so am forced to fix this when the needed arises.
						log.Fatal(err)
					}
					// @todo: this is more related to namespace not needed in subfields, can be improved in recursive
					//          function that generates json
					newFieldJsonCleanup := strings.ReplaceAll(string(newFieldJson),
						fmt.Sprintf(",\"namespace\":\"%s\"", namespace), "")

					if string(existingFieldJson) != newFieldJsonCleanup {
						log.Warn(fmt.Sprintf("Not Matching Field Type [%s] %s    !=    %s",
							newField.Name, existingFieldJson, newFieldJsonCleanup))
						matched = false
					}
				} else {
					if existingField.Type != newField.Type {
						matched = false
					}
				}
			} else {
				log.Warn(fmt.Sprint("Field doesn't exist", newField.Name))
				matched = false
			}
		}
	}

	// // Check if the schema matches
	// log.Info("Schema Matching", existingSchema, "<==>", string(newSubjectSchema))

	// if matched {
	// 	log.Info("Schema Matched")
	// } else {
	// 	log.Info("Schema Not Matched")
	// }
	return matched
}

func (host *SchemaRegistryHost) SetCompatibilityForSubject(subject string) (bool, error) {
	log.Info("Update compatibility of %s in schema registry", subject)

	// RegistryHost has to be passed in a better way then been monkey patched like this
	if host.RegistryHost == "" {
		log.Error("CreateSchemaForSubject No Reg Host:", host.RegistryHost)
		return false, ErrNoRegistryHostDefined
	}

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(host.RegistryHost)

	level, err := schemaRegistryClient.ChangeSubjectCompatibilityLevel(subject, srclient.None)
	if err != nil {
		log.Error("SetCompatibilityForSubject Error:", err.Error())
		return false, err
	}
	log.Info("SetCompatibilityForSubject - Level:", level)
	return true, nil
}

// ApplyAvroEncoding uses the schema registry
func (host *SchemaRegistryHost) ApplyAvroEncoding(namespace string, encoded []byte, err error, name string, encoder sarama.Encoder) ([]byte, error) {

	// If data is not encoded, will need to encode else can simply ignore and return already encoded data
	if len(encoded) == 0 && err == nil {
		// Get Schema Subject <- should cache this so next time it will just use the cache
		schemaSubject := fmt.Sprintf("%s.%s-%s", namespace, name, SubjectPostfixValue)
		schema, err := host.GetSchemaBySubject(schemaSubject) // Should cache this for next time
		if err != nil {
			if errors.Is(err, ErrSubjectNotFound) {
				schema, err = host.CreateSchemaForSubject(schemaSubject, namespace, name, encoder)
				if err != nil {
					return encoded, err
				}
			} else {
				return encoded, err
			}
		}
		// @todo: Need to check the local cache and if the schema was already cached after a match regardless of
		//          whether it matched or not, it shouldn't do this in all calls
		// Match existing schema with new schema
		if !host.MatchSchemaForSubject(schemaSubject, namespace, name, schema.Schema(), encoder) {
			setCompatibility, err := host.SetCompatibilityForSubject(schemaSubject)
			if err != nil {
				return encoded, err
			}
			if !setCompatibility {
				log.Error("Unable to set compatibility", schemaSubject)
				return encoded, err
			}
			schema, err = host.CreateSchemaForSubject(schemaSubject, namespace, name, encoder)
			if err != nil {
				return encoded, err
			}
		}

		// Decode the encoded data, needed to see if the data is already encoded or not
		data := make(map[string]interface{})
		mapConfig := &mapstructure.DecoderConfig{
			TagName: "json",
			Result:  &data,
			Squash:  true,
		}
		// create a new decoder
		decoder, _ := mapstructure.NewDecoder(mapConfig)
		// pass encoder to set the value inside data
		if err := decoder.Decode(encoder); err != nil {
			log.Fatal(err)
		}

		// Start building the encoded data
		// 1. Header
		schemaIDBytes := make([]byte, 4)
		// 2. Schema ID
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		// 3. Schema Data
		value, _ := json.Marshal(data)
		native, _, _ := schema.Codec().NativeFromTextual(value)
		valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

		// Build the record to save in encoded
		var recordValue []byte // as encoded can be null, initialize just in case
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, valueBytes...)

		encoded = recordValue // set the record value to encoded
	}
	return encoded, err
}
