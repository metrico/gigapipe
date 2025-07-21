package model

import (
	"time"
)

// Data
type PrometheusMetric struct {
	Version     uint32 `protobuf:"varint,1,req,name=Version" json:"Version"`
	Protocol    uint32 `protobuf:"varint,2,req,name=Protocol" json:"Protocol"`
	SrcIP       string `protobuf:"bytes,3,req,name=SrcIP" json:"SrcIP"`
	DstIP       string `protobuf:"bytes,4,req,name=DstIP" json:"DstIP"`
	SrcPort     uint32 `protobuf:"varint,5,req,name=SrcPort" json:"SrcPort"`
	DstPort     uint32 `protobuf:"varint,6,req,name=DstPort" json:"DstPort"`
	Tsec        uint32 `protobuf:"varint,7,req,name=Tsec" json:"Tsec"`
	Tmsec       uint32 `protobuf:"varint,8,req,name=Tmsec" json:"Tmsec"`
	ProtoType   uint32 `protobuf:"varint,9,req,name=ProtoType" json:"ProtoType"`
	NodeID      uint32 `protobuf:"varint,10,req,name=NodeID" json:"NodeID"`
	NodePW      string `protobuf:"bytes,11,req,name=NodePW" json:"NodePW"`
	Payload     string `protobuf:"bytes,12,req,name=Payload" json:"Payload"`
	CID         string `protobuf:"bytes,13,req,name=CID" json:"CID"`
	Vlan        uint32 `protobuf:"varint,14,req,name=Vlan" json:"Vlan"`
	ProtoString string
	Timestamp   time.Time
	NodeName    string
	TargetName  string
	SID         string
}
