package wire

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// JSONSchema is the shape an app declares for a type that writes or reads its own JSON, with
// WithJSONSchema or a JSONSchema method on the type (ADR 0076). The codec cannot see inside such a
// type, so the app says what it is once, and the API document states it wherever the type appears.
type JSONSchema struct {
	// Type is the JSON type: "string", "number", "integer", "boolean", "object" or "array", or
	// "any" for a value that is free-form JSON, described as {}.
	Type string
	// Format is the OpenAPI format of a "string", "number" or "integer" type.
	Format string
	// Nullable says the type's own methods can write null (or read it), so the document
	// allows null beside Type.
	Nullable bool
	// Properties are the members an "object" has, each with its own schema, in the order the
	// document lists them. Empty for an object whose members are not stated, described as a bare
	// "object". When it is set, Open says whether members beyond these may appear.
	Properties []JSONProperty
	// Open says an "object" with Properties may carry members that are not listed, which the
	// document states as additionalProperties: true, as for a type that keeps every field a client
	// sent. False states additionalProperties: false: the listed members are all there are. It is
	// only for an object with Properties.
	Open bool
	// Items is the schema of each element of an "array". Nil leaves the elements unstated.
	Items *JSONSchema
}

// JSONProperty is one named member of a JSONSchema of Type "object".
type JSONProperty struct {
	// Name is the member's name on the wire, as the type's own methods write it.
	Name string
	// Schema is the shape of its value.
	Schema JSONSchema
	// Required says a value of the type always has the member.
	Required bool
}

// JSONSchemas maps a type to the schema its author gave it with WithJSONSchema.
type JSONSchemas map[reflect.Type]JSONSchema

// JSONTypes lists the Type values a JSONSchema may have.
var JSONTypes = []string{"string", "number", "integer", "boolean", "object", "array", "any"}

// Check says what is wrong with s, or nil: its own rules, then those of each property and of its
// items. A message names the property by its path from s, such as properties "meta.tags".
func (s JSONSchema) Check() error { return s.check("") }

func (s JSONSchema) check(path string) error {
	at := ""
	if path != "" {
		at = " (property " + strconv.Quote(path) + ")"
	}
	switch {
	case !slices.Contains(JSONTypes, s.Type):
		return errors.New("the type is " + strconv.Quote(s.Type) + ", which is not a JSON type: give one of " + strings.Join(JSONTypes, ", ") + at)
	case s.Format != "" && s.Type != "string" && s.Type != "number" && s.Type != "integer":
		return errors.New("a format belongs to a string, number or integer, and the type is " + strconv.Quote(s.Type) + at)
	case s.Type == "any" && s.Nullable:
		return errors.New("a free-form value already admits null: drop Nullable" + at)
	case (len(s.Properties) > 0 || s.Open) && s.Type != "object":
		return errors.New("Properties and Open belong to an object, and the type is " + strconv.Quote(s.Type) + at)
	case s.Open && len(s.Properties) == 0:
		return errors.New("Open is set but there are no Properties: Open says an object with Properties has members beyond them; for an object whose members are not stated, give Type \"object\" alone" + at)
	case s.Items != nil && s.Type != "array":
		return errors.New("Items belongs to an array, and the type is " + strconv.Quote(s.Type) + at)
	}
	seen := map[string]bool{}
	for _, p := range s.Properties {
		name := p.Name
		if path != "" {
			name = path + "." + p.Name
		}
		switch {
		case p.Name == "":
			return errors.New("a property has no Name: give the member's name on the wire" + at)
		case seen[p.Name]:
			return fmt.Errorf("the property %q is listed twice%s", p.Name, at)
		}
		seen[p.Name] = true
		if err := p.Schema.check(name); err != nil {
			return err
		}
	}
	if s.Items != nil {
		item := "[]"
		if path != "" {
			item = path + "[]"
		}
		return s.Items.check(item)
	}
	return nil
}

// SchemaMethodName is the method a type may have to state its own schema.
const SchemaMethodName = "JSONSchema"

// DeclaredOf returns the schema t states through its own JSONSchema method, and whether it has
// the method. The method is called on a zero value, once, at Registration, so it must not depend
// on the value. An error says the method has the wrong signature or states a schema that is
// not valid, so that the mistake is refused instead of ignored.
func DeclaredOf(t reflect.Type) (JSONSchema, bool, error) {
	m, ok := t.MethodByName(SchemaMethodName)
	recvIsPtr := false
	if !ok {
		m, ok = reflect.PointerTo(t).MethodByName(SchemaMethodName)
		recvIsPtr = true
	}
	if !ok {
		return JSONSchema{}, false, nil
	}
	if mt := m.Type; mt.NumIn() != 1 || mt.NumOut() != 1 || mt.Out(0) != reflect.TypeFor[JSONSchema]() {
		return JSONSchema{}, true, fmt.Errorf("%s has a JSONSchema method that is not func() bites.JSONSchema, so it would never be read: change its signature to return the shape its own JSON methods write, or rename it", t)
	}
	recv := reflect.New(t)
	if !recvIsPtr {
		recv = recv.Elem()
	}
	s := m.Func.Call([]reflect.Value{recv})[0].Interface().(JSONSchema)
	if err := s.Check(); err != nil {
		return JSONSchema{}, true, fmt.Errorf("the JSONSchema method of %s states a schema that is not valid: %w", t, err)
	}
	return s, true, nil
}
