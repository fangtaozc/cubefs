// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) UpdateXAttr(req *proto.UpdateXAttrRequest, p *Packet) (err error) {
	log.LogWarnf("UpdateXAttr not supported in new version, value=%s", req.Value)
	p.PacketErrorWithBody(proto.OpErr, []byte("not supported in new version"))
	return
}

func (mp *metaPartition) SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error) {
	value := []byte(req.Value)
	// 修复T3(内核客户端二进制xattr): ValueBase64为true时Value是调用方(目前
	// 只有内核客户端在真二进制值上会这么做)base64编码过的文本,需要先解码
	// 还原原始字节再存——存储层(下面的extend.Put)本身一直是二进制安全的,
	// 缺的只是"怎么把任意字节安全地放进一个JSON字符串"这一步,base64就是
	// 补这一步。旧客户端/该字段缺省时这里不执行,行为跟这个字段引入前完全
	// 一样。
	if req.ValueBase64 {
		if value, err = base64.StdEncoding.DecodeString(req.Value); err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte("invalid base64 xattr value: "+err.Error()))
			return
		}
	}
	extend := NewExtend(req.Inode)
	extend.Put([]byte(req.Key), value, mp.verSeq)
	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) BatchSetXAttr(req *proto.BatchSetXAttrRequest, p *Packet) (err error) {
	extend := NewExtend(req.Inode)
	for key, val := range req.Attrs {
		extend.Put([]byte(key), []byte(val), mp.verSeq)
	}

	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error) {
	response := &proto.GetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Key:         req.Key,
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		if extend := treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
			if value, exist := extend.Get([]byte(req.Key)); exist {
				// 修复T3(内核客户端二进制xattr): WantBase64为true时把原始字节
				// base64编码后再放进response.Value这个string字段——不这么做的话,
				// 下面json.Marshal对一个持有非法UTF-8字节的string字段会静默替换
				// 成U+FFFD,不管请求方要不要,数据已经在这一步被废了。base64输出
				// 恒为合法ASCII,序列化不会有任何损失,请求方看ValueBase64回显
				// 决定要不要解码。缺省(旧客户端/无这个需求)时行为跟这个字段
				// 引入前完全一样。
				if req.WantBase64 {
					response.Value = base64.StdEncoding.EncodeToString(value)
					response.ValueBase64 = true
				} else {
					response.Value = string(value)
				}
			}
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) GetAllXAttr(req *proto.GetAllXAttrRequest, p *Packet) (err error) {
	response := &proto.GetAllXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Attrs:       make(map[string]string),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		if extend := treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
			for key, val := range extend.dataMap {
				response.Attrs[key] = string(val)
			}
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) BatchGetXAttr(req *proto.BatchGetXAttrRequest, p *Packet) (err error) {
	response := &proto.BatchGetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		XAttrs:      make([]*proto.XAttrInfo, 0, len(req.Inodes)),
	}
	for _, inode := range req.Inodes {
		treeItem := mp.extendTree.Get(NewExtend(inode))
		if treeItem != nil {
			info := &proto.XAttrInfo{
				Inode:  inode,
				XAttrs: make(map[string]string),
			}

			var extend *Extend
			if extend = treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
				for _, key := range req.Keys {
					if val, exist := extend.Get([]byte(key)); exist {
						info.XAttrs[key] = string(val)
					}
				}
			}
			response.XAttrs = append(response.XAttrs, info)
		}
	}
	var encoded []byte
	if encoded, err = json.Marshal(response); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) RemoveXAttr(req *proto.RemoveXAttrRequest, p *Packet) (err error) {
	extend := NewExtend(req.Inode)
	extend.Put([]byte(req.Key), nil, req.VerSeq)
	if _, err = mp.putExtend(opFSMRemoveXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error) {
	response := &proto.ListXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		XAttrs:      make([]string, 0),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		if extend := treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
			extend.Range(func(key, value []byte) bool {
				response.XAttrs = append(response.XAttrs, string(key))
				return true
			})
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) putExtend(op uint32, extend *Extend) (resp interface{}, err error) {
	var marshaled []byte
	if marshaled, err = extend.Bytes(); err != nil {
		return
	}
	resp, err = mp.submit(op, marshaled)
	return
}

func (mp *metaPartition) LockDir(req *proto.LockDirRequest, p *Packet) (err error) {
	req.SubmitTime = time.Now()
	if req.LockId == 0 && req.Lease != 0 {
		req.LockId = proto.GenerateRequestID()
		log.LogWarnf("LockDir: req %s has empyt lkId from old verion.", req.String())
	}

	val, err := json.Marshal(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}

	r, err := mp.submit(opFSMLockDir, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}

	resp := r.(*proto.LockDirResponse)
	status := resp.Status
	var reply []byte
	reply, err = json.Marshal(resp)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return
}
