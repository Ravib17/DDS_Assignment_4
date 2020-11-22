// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"go.mongodb.org/mongo-driver/mongo"
	 "net/http"
	 "bytes"
	 "strconv"
    
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	data string
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string
	inmem    bool
	dbobject *dbObject
	mu sync.Mutex
	m  map[string]string // The key-value store for the system.
	dbport string
	Cid string
	Cnode string
	raft *raft.Raft // The consensus mechanism
	collection *mongo.Collection
	logger *log.Logger

}

// New returns a new Store.
func New(inmem bool , dbport string) *Store {
     dbo := newdbConeection(dbport)
     fmt.Print("create db object")
     collection:= dbo.createCollection("abc","pqr")
     //  data := []byte(`{"a":"m", "c":5, "d": ["e", "f"]}`)
     // dbobject.insertData(data,collection)
     
   
   
	return &Store{
		m:      make(map[string]string),
		inmem:  inmem,
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
		collection : collection,
		dbobject : dbo,
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Store) Open(enableSingle bool, localID string) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if s.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
		if err != nil {
			return fmt.Errorf("new bolt store: %s", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) ([]map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data1 := s.dbobject.getData(key,s.collection)
	// data2, _ := http.Get("http://127.0.0.1:"+string(s.Cnode)+"/key/"+key)
	
	return data1, nil
}

// Set sets the value for the given key.
func (s *Store) Set(data map[string]interface{} ) error {
	

	if s.raft.State() != raft.Leader {
		
    	   postBody, _ := json.Marshal(data)
		   responseBody := bytes.NewBuffer(postBody)
		   i, err := strconv.Atoi(string(s.raft.Leader())[1:])
		   i = i - 1000
		   p := strconv.Itoa(i)

		   resp, err := http.Post("http://127.0.0.1:"+string(p)+"/key", "application/json", responseBody)
		   	fmt.Printf("http://127.0.0.1:"+string(p)+"/key")
		   if err != nil {
		   	fmt.Print(err)
		      return err
		   }
		   fmt.Print(resp)
		   return err
	}
	loc := data["address"].(map[string]interface{})
	if (s.Cid == "1" && loc["city"] == "Banglore") || ( s.Cid == "2" && loc["city"] == "Hyderabad")  {

		  postBody, _ := json.Marshal(data)
		   responseBody := bytes.NewBuffer(postBody)
		   p := s.Cnode

		   resp, err := http.Post("http://127.0.0.1:"+string(p)+"/key", "application/json", responseBody)
		   	fmt.Printf("http://127.0.0.1:"+string(p)+"/key")
		   if err != nil {
		   	fmt.Print(err)
		      return err
		   }
		   fmt.Print(resp)
		   return err

	} 
	data["op"]="set"
	d , err := json.Marshal(data)
	e:= string(d)
	fmt.Printf(e)
	c := &command{
		Op:    "set",
		data: `{'name':'abc'}`,

	}
	fmt.Printf(c.data)
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	fmt.Print("\n\n\n")
	fa := make(map[string]interface{})
	json.Unmarshal(b, &fa);
	
	fmt.Print(fa)
	fmt.Print("\n")
	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key1 string,key2 string) error {

	fmt.Println("In Delete")
	if s.raft.State() != raft.Leader {
		

	// Create request
	i, err := strconv.Atoi(string(s.raft.Leader())[1:])
	i = i - 1000
	p := strconv.Itoa(i)
    client := &http.Client{}
	req, err := http.NewRequest("DELETE","http://127.0.0.1:"+string(p)+"/key"+"/"+key1+"/"+key2,nil)

    resp, err := client.Do(req)
    if err != nil {
        fmt.Println(err)
        return err
    }
    defer resp.Body.Close()
	return err
   }
	dataobj := make(map[string]interface{})
	dataobj["name"] = key1
	dataobj["city"]=key2
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	dataobj["op"]="delete"
	d , err := json.Marshal(dataobj)
	e:= string(d)
	fmt.Printf(e)
	b, err := json.Marshal(dataobj)
	if err != nil {
		return err
	}
	fmt.Print("\n\n\n")
	fa := make(map[string]interface{})
	json.Unmarshal(b, &fa);
	
	fmt.Print(fa)
	fmt.Print("\n")
	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}


// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	c:= make(map[string]interface{})
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}
	fmt.Print("check\n")
 	fmt.Print(c["op"].(string))
 	fmt.Print("done\n")
 	
	val := c["op"].(string)
	fmt.Print(val)
	switch val {
	case "set":
		return f.applySet(c)
	case "delete":
		return f.applyDelete(c)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c["Op"]))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(data map[string]interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Print("\n\n\n\n adsfssfdf \n\n]n")
	f.dbobject.insertData(data,f.collection)
	return nil
}

func (f *fsm) applyDelete(data map[string]interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Println("Dataaaaa")
	f.dbobject.deleteData(data,f.collection)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
