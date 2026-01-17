package config

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"

	"gopkg.in/yaml.v3"
)

const (
	defaultTag = "default"
)

// Load loads configuration from a YAML file into a struct of type T.
// If the file does not exist, it creates one with default values.
func Load[T any](configFile string) (*T, error) {
	c := new(T)
	if err := LoadInto(configFile, c); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadInto loads configuration into an existing struct pointer.
// It applies defaults to empty fields first, then merges with the file content.
// If the file doesn't exist, it writes the current state (defaults + pre-set values) to the file.
func LoadInto[T any](configFile string, c *T) error {
	// 1. Apply Defaults (only to zero values)
	if err := SetDefaults(c, defaultTag); err != nil {
		return fmt.Errorf("failed to set defaults: %w", err)
	}

	// 2. Read Existing Config (if exists)
	if _, err := os.Stat(configFile); err == nil {
		data, err := os.ReadFile(configFile)
		if err != nil {
			return fmt.Errorf("failed to read config file: %w", err)
		}

		// Unmarshal merges into the existing struct 'c'
		if err := yaml.Unmarshal(data, c); err != nil {
			return fmt.Errorf("failed to parse config file: %w", err)
		}

		// 3. Write Config Back (Persist Defaults / Updates)
		// This ensures that if the user deleted a key, it comes back with the default value.
		newData, err := yaml.Marshal(c)
		if err != nil {
			return fmt.Errorf("failed to marshal config: %w", err)
		}

		if err := os.WriteFile(configFile, newData, 0644); err != nil {
			log.Printf("Warning: Failed to write config file: %v", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking config file: %w", err)
	} else {
		log.Printf("Config file not found, creating default at %s", configFile)

		// 3. Write Config Back (Persist Defaults if file didn't exist)
		data, err := yaml.Marshal(c)
		if err != nil {
			return fmt.Errorf("failed to marshal config: %w", err)
		}

		if err := os.WriteFile(configFile, data, 0644); err != nil {
			log.Printf("Warning: Failed to write config file: %v", err)
		}
	}

	return nil
}

// SetDefaults iterates over struct fields and sets values from 'default' tag.
// It is exported to allow use on existing structs.
func SetDefaults(ptr interface{}, defaultTag string) error {
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		return fmt.Errorf("not a pointer")
	}

	v := reflect.ValueOf(ptr).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// Recursively set defaults for nested structs
		if value.Kind() == reflect.Struct {
			if err := SetDefaults(value.Addr().Interface(), defaultTag); err != nil {
				return err
			}
			continue
		}

		// Handle Slices: Generate one example item if slice is empty
		if value.Kind() == reflect.Slice {
			if value.Len() == 0 {
				elemType := value.Type().Elem()

				// Handle Slice of Structs
				if elemType.Kind() == reflect.Struct {
					newElem := reflect.New(elemType)
					if err := SetDefaults(newElem.Interface(), defaultTag); err != nil {
						return err
					}
					value.Set(reflect.Append(value, newElem.Elem()))
				} else {
					// Handle Slice of Primitives (String, Int, etc.)
					// Append one zero-value item so it appears in the generated config
					zeroElem := reflect.Zero(elemType)
					value.Set(reflect.Append(value, zeroElem))
				}
			}
			continue
		}

		// Handle Maps: Generate one example item if map is nil
		if value.Kind() == reflect.Map {
			if value.IsNil() {
				mapType := value.Type()
				newMap := reflect.MakeMap(mapType)

				// Create Example Key
				var key reflect.Value
				if mapType.Key().Kind() == reflect.String {
					key = reflect.ValueOf("example_key")
				} else {
					key = reflect.Zero(mapType.Key())
				}

				// Create Example Value
				elemType := mapType.Elem()
				newValPtr := reflect.New(elemType) // Pointer to new value

				// Recursively set defaults for complex types
				if elemType.Kind() == reflect.Struct || elemType.Kind() == reflect.Slice || elemType.Kind() == reflect.Map {
					if err := SetDefaults(newValPtr.Interface(), defaultTag); err != nil {
						return err
					}
				} else if elemType.Kind() == reflect.String {
					newValPtr.Elem().SetString("example_value")
				}

				newMap.SetMapIndex(key, newValPtr.Elem())
				value.Set(newMap)
			}
			continue
		}

		defaultVal := field.Tag.Get(defaultTag)
		if defaultVal != "" {
			if err := setField(value, defaultVal); err != nil {
				return fmt.Errorf("failed to set field %s: %w", field.Name, err)
			}
		}
	}
	return nil
}

func setField(value reflect.Value, defaultVal string) error {
	switch value.Kind() {
	case reflect.String:
		if value.String() == "" {
			value.SetString(defaultVal)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Int() == 0 {
			i, err := strconv.Atoi(defaultVal)
			if err != nil {
				return err
			}
			value.SetInt(int64(i))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.Uint() == 0 {
			i, err := strconv.ParseUint(defaultVal, 10, 64)
			if err != nil {
				return err
			}
			value.SetUint(i)
		}
	case reflect.Float32, reflect.Float64:
		if value.Float() == 0 {
			f, err := strconv.ParseFloat(defaultVal, 64)
			if err != nil {
				return err
			}
			value.SetFloat(f)
		}
	case reflect.Bool:
		if !value.Bool() {
			b, err := strconv.ParseBool(defaultVal)
			if err != nil {
				return err
			}
			value.SetBool(b)
		}
		// Add other types as needed
	}
	return nil
}
