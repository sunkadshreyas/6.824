package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]string
	appendMap map[int64](map[int]string)
	putMap map[int64]int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.data[args.Key]; ok {
		reply.Value = val
		return
	}
	reply.Value = ""
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if latestReqID, ok := kv.putMap[args.ClientID]; ok && latestReqID >= args.RequestID {
		return
	}
	kv.putMap[args.ClientID] = args.RequestID
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.appendMap[args.ClientID]; !ok {
		kv.appendMap[args.ClientID] = make(map[int]string)
	}
	if val, ok := kv.appendMap[args.ClientID][args.RequestID]; ok {
		reply.Value = val
		return
	}
	kv.appendMap[args.ClientID][args.RequestID] = kv.data[args.Key]
	reply.Value = kv.data[args.Key]
	kv.data[args.Key] += args.Value
	delete(kv.appendMap[args.ClientID], args.RequestID - 1)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.putMap = make(map[int64]int)
	kv.appendMap = make(map[int64]map[int]string)

	return kv
}
