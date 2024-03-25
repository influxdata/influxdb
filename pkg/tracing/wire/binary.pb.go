// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v4.24.3
// source: binary.proto

package wire

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type FieldType int32

const (
	FieldType_FieldTypeString   FieldType = 0
	FieldType_FieldTypeBool     FieldType = 1
	FieldType_FieldTypeInt64    FieldType = 2
	FieldType_FieldTypeUint64   FieldType = 3
	FieldType_FieldTypeDuration FieldType = 4
	FieldType_FieldTypeFloat64  FieldType = 6
)

// Enum value maps for FieldType.
var (
	FieldType_name = map[int32]string{
		0: "FieldTypeString",
		1: "FieldTypeBool",
		2: "FieldTypeInt64",
		3: "FieldTypeUint64",
		4: "FieldTypeDuration",
		6: "FieldTypeFloat64",
	}
	FieldType_value = map[string]int32{
		"FieldTypeString":   0,
		"FieldTypeBool":     1,
		"FieldTypeInt64":    2,
		"FieldTypeUint64":   3,
		"FieldTypeDuration": 4,
		"FieldTypeFloat64":  6,
	}
)

func (x FieldType) Enum() *FieldType {
	p := new(FieldType)
	*p = x
	return p
}

func (x FieldType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FieldType) Descriptor() protoreflect.EnumDescriptor {
	return file_binary_proto_enumTypes[0].Descriptor()
}

func (FieldType) Type() protoreflect.EnumType {
	return &file_binary_proto_enumTypes[0]
}

func (x FieldType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FieldType.Descriptor instead.
func (FieldType) EnumDescriptor() ([]byte, []int) {
	return file_binary_proto_rawDescGZIP(), []int{0}
}

type SpanContext struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TraceID uint64 `protobuf:"varint,1,opt,name=TraceID,proto3" json:"TraceID,omitempty"`
	SpanID  uint64 `protobuf:"varint,2,opt,name=SpanID,proto3" json:"SpanID,omitempty"`
}

func (x *SpanContext) Reset() {
	*x = SpanContext{}
	if protoimpl.UnsafeEnabled {
		mi := &file_binary_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SpanContext) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SpanContext) ProtoMessage() {}

func (x *SpanContext) ProtoReflect() protoreflect.Message {
	mi := &file_binary_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SpanContext.ProtoReflect.Descriptor instead.
func (*SpanContext) Descriptor() ([]byte, []int) {
	return file_binary_proto_rawDescGZIP(), []int{0}
}

func (x *SpanContext) GetTraceID() uint64 {
	if x != nil {
		return x.TraceID
	}
	return 0
}

func (x *SpanContext) GetSpanID() uint64 {
	if x != nil {
		return x.SpanID
	}
	return 0
}

type Span struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Context      *SpanContext           `protobuf:"bytes,1,opt,name=context,proto3" json:"context,omitempty"`
	ParentSpanID uint64                 `protobuf:"varint,2,opt,name=ParentSpanID,proto3" json:"ParentSpanID,omitempty"`
	Name         string                 `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Start        *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=Start,proto3" json:"Start,omitempty"`
	Labels       []string               `protobuf:"bytes,5,rep,name=labels,proto3" json:"labels,omitempty"`
	Fields       []*Field               `protobuf:"bytes,6,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (x *Span) Reset() {
	*x = Span{}
	if protoimpl.UnsafeEnabled {
		mi := &file_binary_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Span) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Span) ProtoMessage() {}

func (x *Span) ProtoReflect() protoreflect.Message {
	mi := &file_binary_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Span.ProtoReflect.Descriptor instead.
func (*Span) Descriptor() ([]byte, []int) {
	return file_binary_proto_rawDescGZIP(), []int{1}
}

func (x *Span) GetContext() *SpanContext {
	if x != nil {
		return x.Context
	}
	return nil
}

func (x *Span) GetParentSpanID() uint64 {
	if x != nil {
		return x.ParentSpanID
	}
	return 0
}

func (x *Span) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Span) GetStart() *timestamppb.Timestamp {
	if x != nil {
		return x.Start
	}
	return nil
}

func (x *Span) GetLabels() []string {
	if x != nil {
		return x.Labels
	}
	return nil
}

func (x *Span) GetFields() []*Field {
	if x != nil {
		return x.Fields
	}
	return nil
}

type Trace struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Spans []*Span `protobuf:"bytes,1,rep,name=spans,proto3" json:"spans,omitempty"`
}

func (x *Trace) Reset() {
	*x = Trace{}
	if protoimpl.UnsafeEnabled {
		mi := &file_binary_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Trace) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Trace) ProtoMessage() {}

func (x *Trace) ProtoReflect() protoreflect.Message {
	mi := &file_binary_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Trace.ProtoReflect.Descriptor instead.
func (*Trace) Descriptor() ([]byte, []int) {
	return file_binary_proto_rawDescGZIP(), []int{2}
}

func (x *Trace) GetSpans() []*Span {
	if x != nil {
		return x.Spans
	}
	return nil
}

type Field struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       string    `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	FieldType FieldType `protobuf:"varint,2,opt,name=FieldType,proto3,enum=wire.FieldType" json:"FieldType,omitempty"`
	// Types that are assignable to Value:
	//
	//	*Field_NumericVal
	//	*Field_StringVal
	Value isField_Value `protobuf_oneof:"value"`
}

func (x *Field) Reset() {
	*x = Field{}
	if protoimpl.UnsafeEnabled {
		mi := &file_binary_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Field) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Field) ProtoMessage() {}

func (x *Field) ProtoReflect() protoreflect.Message {
	mi := &file_binary_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Field.ProtoReflect.Descriptor instead.
func (*Field) Descriptor() ([]byte, []int) {
	return file_binary_proto_rawDescGZIP(), []int{3}
}

func (x *Field) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Field) GetFieldType() FieldType {
	if x != nil {
		return x.FieldType
	}
	return FieldType_FieldTypeString
}

func (m *Field) GetValue() isField_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (x *Field) GetNumericVal() int64 {
	if x, ok := x.GetValue().(*Field_NumericVal); ok {
		return x.NumericVal
	}
	return 0
}

func (x *Field) GetStringVal() string {
	if x, ok := x.GetValue().(*Field_StringVal); ok {
		return x.StringVal
	}
	return ""
}

type isField_Value interface {
	isField_Value()
}

type Field_NumericVal struct {
	NumericVal int64 `protobuf:"fixed64,3,opt,name=NumericVal,proto3,oneof"`
}

type Field_StringVal struct {
	StringVal string `protobuf:"bytes,4,opt,name=StringVal,proto3,oneof"`
}

func (*Field_NumericVal) isField_Value() {}

func (*Field_StringVal) isField_Value() {}

var File_binary_proto protoreflect.FileDescriptor

var file_binary_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04,
	0x77, 0x69, 0x72, 0x65, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x3f, 0x0a, 0x0b, 0x53, 0x70, 0x61, 0x6e, 0x43, 0x6f, 0x6e,
	0x74, 0x65, 0x78, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x54, 0x72, 0x61, 0x63, 0x65, 0x49, 0x44, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x54, 0x72, 0x61, 0x63, 0x65, 0x49, 0x44, 0x12, 0x16,
	0x0a, 0x06, 0x53, 0x70, 0x61, 0x6e, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06,
	0x53, 0x70, 0x61, 0x6e, 0x49, 0x44, 0x22, 0xda, 0x01, 0x0a, 0x04, 0x53, 0x70, 0x61, 0x6e, 0x12,
	0x2b, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x11, 0x2e, 0x77, 0x69, 0x72, 0x65, 0x2e, 0x53, 0x70, 0x61, 0x6e, 0x43, 0x6f, 0x6e, 0x74,
	0x65, 0x78, 0x74, 0x52, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x12, 0x22, 0x0a, 0x0c,
	0x50, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x53, 0x70, 0x61, 0x6e, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x0c, 0x50, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x53, 0x70, 0x61, 0x6e, 0x49, 0x44,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x12, 0x30, 0x0a, 0x05, 0x53, 0x74, 0x61, 0x72, 0x74, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x05, 0x53, 0x74, 0x61, 0x72, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73,
	0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x12, 0x23,
	0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b,
	0x2e, 0x77, 0x69, 0x72, 0x65, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x52, 0x06, 0x66, 0x69, 0x65,
	0x6c, 0x64, 0x73, 0x22, 0x29, 0x0a, 0x05, 0x54, 0x72, 0x61, 0x63, 0x65, 0x12, 0x20, 0x0a, 0x05,
	0x73, 0x70, 0x61, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x77, 0x69,
	0x72, 0x65, 0x2e, 0x53, 0x70, 0x61, 0x6e, 0x52, 0x05, 0x73, 0x70, 0x61, 0x6e, 0x73, 0x22, 0x93,
	0x01, 0x0a, 0x05, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2d, 0x0a, 0x09, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0f, 0x2e,
	0x77, 0x69, 0x72, 0x65, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09,
	0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x20, 0x0a, 0x0a, 0x4e, 0x75, 0x6d,
	0x65, 0x72, 0x69, 0x63, 0x56, 0x61, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x10, 0x48, 0x00, 0x52,
	0x0a, 0x4e, 0x75, 0x6d, 0x65, 0x72, 0x69, 0x63, 0x56, 0x61, 0x6c, 0x12, 0x1e, 0x0a, 0x09, 0x53,
	0x74, 0x72, 0x69, 0x6e, 0x67, 0x56, 0x61, 0x6c, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00,
	0x52, 0x09, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x56, 0x61, 0x6c, 0x42, 0x07, 0x0a, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x2a, 0x89, 0x01, 0x0a, 0x09, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79,
	0x70, 0x65, 0x12, 0x13, 0x0a, 0x0f, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x53,
	0x74, 0x72, 0x69, 0x6e, 0x67, 0x10, 0x00, 0x12, 0x11, 0x0a, 0x0d, 0x46, 0x69, 0x65, 0x6c, 0x64,
	0x54, 0x79, 0x70, 0x65, 0x42, 0x6f, 0x6f, 0x6c, 0x10, 0x01, 0x12, 0x12, 0x0a, 0x0e, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x49, 0x6e, 0x74, 0x36, 0x34, 0x10, 0x02, 0x12, 0x13,
	0x0a, 0x0f, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x55, 0x69, 0x6e, 0x74, 0x36,
	0x34, 0x10, 0x03, 0x12, 0x15, 0x0a, 0x11, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65,
	0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x10, 0x04, 0x12, 0x14, 0x0a, 0x10, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x46, 0x6c, 0x6f, 0x61, 0x74, 0x36, 0x34, 0x10, 0x06,
	0x42, 0x08, 0x5a, 0x06, 0x2e, 0x3b, 0x77, 0x69, 0x72, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_binary_proto_rawDescOnce sync.Once
	file_binary_proto_rawDescData = file_binary_proto_rawDesc
)

func file_binary_proto_rawDescGZIP() []byte {
	file_binary_proto_rawDescOnce.Do(func() {
		file_binary_proto_rawDescData = protoimpl.X.CompressGZIP(file_binary_proto_rawDescData)
	})
	return file_binary_proto_rawDescData
}

var file_binary_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_binary_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_binary_proto_goTypes = []interface{}{
	(FieldType)(0),                // 0: wire.FieldType
	(*SpanContext)(nil),           // 1: wire.SpanContext
	(*Span)(nil),                  // 2: wire.Span
	(*Trace)(nil),                 // 3: wire.Trace
	(*Field)(nil),                 // 4: wire.Field
	(*timestamppb.Timestamp)(nil), // 5: google.protobuf.Timestamp
}
var file_binary_proto_depIdxs = []int32{
	1, // 0: wire.Span.context:type_name -> wire.SpanContext
	5, // 1: wire.Span.Start:type_name -> google.protobuf.Timestamp
	4, // 2: wire.Span.fields:type_name -> wire.Field
	2, // 3: wire.Trace.spans:type_name -> wire.Span
	0, // 4: wire.Field.FieldType:type_name -> wire.FieldType
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_binary_proto_init() }
func file_binary_proto_init() {
	if File_binary_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_binary_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SpanContext); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_binary_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Span); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_binary_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Trace); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_binary_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Field); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_binary_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*Field_NumericVal)(nil),
		(*Field_StringVal)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_binary_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_binary_proto_goTypes,
		DependencyIndexes: file_binary_proto_depIdxs,
		EnumInfos:         file_binary_proto_enumTypes,
		MessageInfos:      file_binary_proto_msgTypes,
	}.Build()
	File_binary_proto = out.File
	file_binary_proto_rawDesc = nil
	file_binary_proto_goTypes = nil
	file_binary_proto_depIdxs = nil
}
