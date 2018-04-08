// Code generated by protoc-gen-go. DO NOT EDIT.
// source: src/appsinstalled.proto

/*
Package appsinstalled is a generated protocol buffer package.

It is generated from these files:
	src/appsinstalled.proto

It has these top-level messages:
	UserApps
*/
package appsinstalled

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type UserApps struct {
	Apps             []uint32 `protobuf:"varint,1,rep,name=apps" json:"apps,omitempty"`
	Lat              *float64 `protobuf:"fixed64,2,opt,name=lat" json:"lat,omitempty"`
	Lon              *float64 `protobuf:"fixed64,3,opt,name=lon" json:"lon,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *UserApps) Reset()                    { *m = UserApps{} }
func (m *UserApps) String() string            { return proto.CompactTextString(m) }
func (*UserApps) ProtoMessage()               {}
func (*UserApps) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *UserApps) GetApps() []uint32 {
	if m != nil {
		return m.Apps
	}
	return nil
}

func (m *UserApps) GetLat() float64 {
	if m != nil && m.Lat != nil {
		return *m.Lat
	}
	return 0
}

func (m *UserApps) GetLon() float64 {
	if m != nil && m.Lon != nil {
		return *m.Lon
	}
	return 0
}

func init() {
	proto.RegisterType((*UserApps)(nil), "UserApps")
}

func init() { proto.RegisterFile("src/appsinstalled.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 101 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x2f, 0x2e, 0x4a, 0xd6,
	0x4f, 0x2c, 0x28, 0x28, 0xce, 0xcc, 0x2b, 0x2e, 0x49, 0xcc, 0xc9, 0x49, 0x4d, 0xd1, 0x2b, 0x28,
	0xca, 0x2f, 0xc9, 0x57, 0x72, 0xe2, 0xe2, 0x08, 0x2d, 0x4e, 0x2d, 0x72, 0x2c, 0x28, 0x28, 0x16,
	0x12, 0xe2, 0x62, 0x01, 0x29, 0x91, 0x60, 0x54, 0x60, 0xd6, 0xe0, 0x0d, 0x02, 0xb3, 0x85, 0x04,
	0xb8, 0x98, 0x73, 0x12, 0x4b, 0x24, 0x98, 0x14, 0x18, 0x35, 0x18, 0x83, 0x40, 0x4c, 0xb0, 0x48,
	0x7e, 0x9e, 0x04, 0x33, 0x54, 0x24, 0x3f, 0x0f, 0x10, 0x00, 0x00, 0xff, 0xff, 0x96, 0x23, 0x5f,
	0xca, 0x5d, 0x00, 0x00, 0x00,
}
