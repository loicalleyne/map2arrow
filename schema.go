package map2arrow

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
)

type FieldPos struct {
	parent       *FieldPos
	name         string
	path         []string
	field        arrow.Field
	AppendFunc   func(val interface{}) error
	children     []*FieldPos
	index, depth int32
	err          error
}

var ErrUndefinedFieldType = errors.New("could not determine type of unpopulated field")

func NewFieldPos() *FieldPos { return &FieldPos{index: -1} }

func (f *FieldPos) Name() string { return f.name }

func (f *FieldPos) Error() error { return f.err }

func (f *FieldPos) Child(index int) (*FieldPos, error) {
	if index < len(f.Children()) {
		return f.children[index], nil
	}
	return nil, fmt.Errorf("%v child index %d not found", f.NamePath(), index)
}

func (f *FieldPos) Children() []*FieldPos { return f.children }

func (f *FieldPos) Metadata() arrow.Metadata { return f.field.Metadata }

func (f *FieldPos) NewChild(childName string) *FieldPos {
	var child FieldPos = FieldPos{
		parent: f,
		name:   childName,
		index:  int32(len(f.children)),
		depth:  f.depth + 1,
	}
	child.path = child.NamePath()
	f.children = append(f.children, &child)
	return &child
}

// NamePath returns a slice of keys making up the path to the field
func (f *FieldPos) NamePath() []string {
	if len(f.path) == 0 {
		var path []string
		cur := f
		for i := f.depth - 1; i >= 0; i-- {
			path = append([]string{cur.name}, path...)
			cur = cur.parent
		}
		return path
	}
	return f.path
}

// GetValue retrieves the value from the map[string]interface{}
// by following the field's key path
func (f *FieldPos) GetValue(m map[string]interface{}) interface{} {
	var value interface{} = m
	for _, key := range f.NamePath() {
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return nil
		}
		value, ok = valueMap[key]
		if !ok {
			return nil
		}
	}
	return value
}

// Map2Arrow returns an Arrow schema generated from the structure/types of
// a map[string]interface{}. It is expected that the input map be fully populated;
// an error is returned along with the schema if undefined fields are found, any unpopulated
// fields or slices will be defined as arrow.Binary to avoid data loss if the schema is still used.
func Map2Arrow(m map[string]interface{}) (*arrow.Schema, error) {
	f := NewFieldPos()
	mapToArrow(f, m)
	var fields []arrow.Field
	for _, c := range f.Children() {
		fields = append(fields, c.field)
	}
	err := errWrap(f)
	return arrow.NewSchema(fields, nil), err
}

func errWrap(f *FieldPos) error {
	var err error
	if f.err != nil {
		err = errors.Join(f.err)
	}
	if len(f.Children()) > 0 {
		for _, field := range f.Children() {
			err = errors.Join(errWrap(field))
		}
	}
	return err
}

func mapToArrow(f *FieldPos, m map[string]interface{}) {
	for k, v := range m {
		child := f.NewChild(k)
		switch t := v.(type) {
		case map[string]interface{}:
			mapToArrow(child, t)
			var fields []arrow.Field
			for _, c := range child.Children() {
				fields = append(fields, c.field)
			}
			child.field = arrow.Field{Name: k, Type: arrow.StructOf(fields...), Nullable: true}
		case []interface{}:
			if len(t) <= 0 {
				child.err = fmt.Errorf("%v : %v", ErrUndefinedFieldType, child.NamePath())
				child.field = arrow.Field{Name: k, Type: arrow.BinaryTypes.Binary, Nullable: true}
			} else {
				et := sliceElemType(child, t)
				child.field = arrow.Field{Name: k, Type: arrow.ListOf(et), Nullable: true}
			}
		case nil:
			child.err = fmt.Errorf("%v : %v", ErrUndefinedFieldType, child.NamePath())
			child.field = arrow.Field{Name: k, Type: goType2Arrow(child, v), Nullable: true}
		default:
			child.field = arrow.Field{Name: k, Type: goType2Arrow(child, v), Nullable: true}
		}
	}
	var fields []arrow.Field
	for _, c := range f.Children() {
		fields = append(fields, c.field)
	}
	f.field = arrow.Field{Name: f.name, Type: arrow.StructOf(fields...), Nullable: true}
}

func sliceElemType(f *FieldPos, v []interface{}) arrow.DataType {
	switch ft := v[0].(type) {
	case map[string]interface{}:
		child := f.NewChild(f.name + ".elem")
		mapToArrow(child, ft)
		var fields []arrow.Field
		for _, c := range child.Children() {
			fields = append(fields, c.field)
		}
		return arrow.StructOf(fields...)
	case []interface{}:
		if len(ft) <= 0 {
			f.err = fmt.Errorf("%v : %v", ErrUndefinedFieldType, f.NamePath())
			return arrow.BinaryTypes.Binary
		}
		child := f.NewChild(f.name + ".elem")
		et := sliceElemType(child, ft)
		return arrow.ListOf(et)
	default:
		return goType2Arrow(f, v)
	}
	return nil
}

func goType2Arrow(f *FieldPos, gt any) arrow.DataType {
	var dt arrow.DataType
	switch gt.(type) {
	// either 32 or 64 bits
	case int:
		dt = arrow.PrimitiveTypes.Int64
	// the set of all signed  8-bit integers (-128 to 127)
	case int8:
		dt = arrow.PrimitiveTypes.Int8
	// the set of all signed 16-bit integers (-32768 to 32767)
	case int16:
		dt = arrow.PrimitiveTypes.Int16
	// the set of all signed 32-bit integers (-2147483648 to 2147483647)
	case int32:
		dt = arrow.PrimitiveTypes.Int32
	// the set of all signed 64-bit integers (-9223372036854775808 to 9223372036854775807)
	case int64:
		dt = arrow.PrimitiveTypes.Int64
	// either 32 or 64 bits
	case uint:
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all unsigned  8-bit integers (0 to 255)
	case uint8:
		dt = arrow.PrimitiveTypes.Uint8
	// the set of all unsigned 16-bit integers (0 to 65535)
	case uint16:
		dt = arrow.PrimitiveTypes.Uint16
	// the set of all unsigned 32-bit integers (0 to 4294967295)
	case uint32:
		dt = arrow.PrimitiveTypes.Uint32
	// the set of all unsigned 64-bit integers (0 to 18446744073709551615)
	case uint64:
		dt = arrow.PrimitiveTypes.Uint64
	// the set of all IEEE-754 32-bit floating-point numbers
	case float32:
		dt = arrow.PrimitiveTypes.Float32
	// the set of all IEEE-754 64-bit floating-point numbers
	case float64:
		dt = arrow.PrimitiveTypes.Float64
	case bool:
		dt = arrow.FixedWidthTypes.Boolean
	case string:
		dt = arrow.BinaryTypes.String
	case []byte:
		dt = arrow.BinaryTypes.Binary
	// the set of all complex numbers with float32 real and imaginary parts
	case complex64:
		// TO-DO
	// the set of all complex numbers with float64 real and imaginary parts
	case complex128:
		// TO-DO
	case nil:
		f.err = fmt.Errorf("%v : %v", ErrUndefinedFieldType, f.NamePath())
		dt = arrow.BinaryTypes.Binary
	}
	return dt
}
