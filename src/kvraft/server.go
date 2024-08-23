package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op OpType
	Key string
	Value string
	ClientID int64
	RequestID int
}

type Result struct {
	index int
	term int
	value string
	err Err
}

type Req struct {
	RequestID int
	Value string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	ps *raft.Persister
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	clientReqMap map[int64]Req
	chanMap map[int64]chan Result
}

func getChannelID(term, index int) int64 {
	id := int64(term) << 32
	id += int64(index)
	return id
}

func (kv *KVServer) createChannel(term, index int) chan Result {
	chanID := getChannelID(term, index)
	ch := make(chan Result, 1)
	kv.chanMap[chanID] = ch
	return ch
}

func (kv *KVServer) deleteChannel(term, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	chanID := getChannelID(term, index)
	close(kv.chanMap[chanID])
	delete(kv.chanMap, chanID)
}

func (kv *KVServer) isReqPresent(clientID int64, requestID int) (bool, string, Err) {
	reqEntry, ok := kv.clientReqMap[clientID]
	if ok && reqEntry.RequestID >= requestID {
		return true, reqEntry.Value, reqEntry.Err
	}
	return false, "", ErrWrongLeader
}

func (kv *KVServer) encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientReqMap)
	return w.Bytes()
}

func (kv *KVServer) decode(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	var dataMap map[string]string
	var reqMap map[int64]Req

	if d.Decode(&dataMap) != nil || d.Decode(&reqMap) != nil {
		log.Fatalf("error when decoding data and client req map")
		return
	}

	kv.data = dataMap
	kv.clientReqMap = reqMap
}

func (kv *KVServer) startRaft(key, value string, op OpType, clientID int64, reqID int, ch chan GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if isPresent, val, err := kv.isReqPresent(clientID, reqID); isPresent {
		ch <- GetReply{Err: err, Value: val}
		return
	}

	command := Op{
		Op: op,
		Key: key,
		Value: value,
		ClientID: clientID,
		RequestID: reqID,
	}
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		ch <- GetReply{Err: ErrWrongLeader, Value: ""}
		return
	}
	resch := kv.createChannel(term, index)
	go kv.waitRaft(term, index, ch, resch)
}

func (kv *KVServer) waitRaft(term, index int, ch chan GetReply, resCh chan Result) {
	timer := time.NewTimer(500 * time.Millisecond)
	select {
	case <- timer.C:
		ch <- GetReply{Err: ErrWrongLeader, Value: ""}
	case res := <- resCh:
		ch <- GetReply{Err: res.err, Value: res.value}
	}
	kv.deleteChannel(term, index)
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ch := make(chan GetReply)
	go kv.startRaft(args.Key, "", GET, args.ClientID, args.RequestID, ch)
	r := <- ch
	reply.Value = r.Value
	reply.Err = r.Err
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ch := make(chan GetReply)
	go kv.startRaft(args.Key, args.Value, PUT, args.ClientID, args.RequestID, ch)
	r := <- ch
	reply.Err = r.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ch := make(chan GetReply)
	go kv.startRaft(args.Key, args.Value, APPEND, args.ClientID, args.RequestID, ch)
	r := <- ch
	reply.Err = r.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Run() {
	for !kv.killed() {
		var index, term int
		msg := <- kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			index = msg.CommandIndex
			term = msg.CommandTerm
			op := msg.Command.(Op)
			opType, key, val, clientId, reqId := op.Op, op.Key, op.Value, op.ClientID, op.RequestID
			var err Err
			if isPresent, existingVal, existingErr := kv.isReqPresent(clientId, reqId); isPresent {
				err = existingErr
				val = existingVal
			} else {
				if opType == GET {
					mapValue, isKeyPresent := kv.data[key]
					if isKeyPresent {
						val = mapValue
						err = OK
					} else {
						val = ""
						err = ErrNoKey
					}
				} else if opType == PUT {
					kv.data[key] = val
					err = OK
				} else if opType == APPEND{
					kv.data[key] += val
					err = OK
				} else {
					log.Fatalf("invalid operation type %v", opType)
				}
				if _, ok := kv.clientReqMap[clientId]; !ok {
					kv.clientReqMap[clientId] = Req{}
				}
				clientReqMap := Req {
					RequestID: reqId,
					Value: val,
					Err: err,
				}
				kv.clientReqMap[clientId] = clientReqMap
				if kv.maxraftstate != -1 && kv.maxraftstate < kv.ps.RaftStateSize() {
					kv.rf.Snapshot(index, kv.encode())
				}
			}
			if ch, ok := kv.chanMap[getChannelID(term, index)]; ok {
				select {
				case ch <- Result{index: index, term: term, value: val, err: err}:
				default:
				}
			}
		} else if msg.SnapshotValid {
			kv.decode(msg.Snapshot)
		} else {
			log.Fatalf("invalid msg %+v\n", msg)
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ps = persister

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.clientReqMap = make(map[int64]Req)
	kv.chanMap = make(map[int64]chan Result)

	kv.decode(kv.ps.ReadSnapshot())

	go kv.Run()

	return kv
}
