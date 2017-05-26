package pets

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Pets struct {
	sync.RWMutex
	//name container
	shortMap map[string][]string //short name to long names
	nameMap  map[string][]byte   //long name to value
	//zk instance
	addr   string   //zk address
	conn   *zk.Conn //zookeeper instance connection
	stopCh chan bool
}

func InitPetsInstance(connStr string) (*Pets, error) {
	pets := new(Pets)
	pets.addr = connStr
	pets.shortMap = make(map[string][]string)
	pets.nameMap = make(map[string][]byte)
	servers := strings.Split(connStr, ",")
	conn, ec, err := zk.Connect(servers, 10*time.Second)
	if err != nil {
		return nil, err
	}
	pets.conn = conn
	pets.stopCh = make(chan bool)
	go pets.CheckConn(ec)
	return pets, nil
}

func (pets *Pets) CheckConn(event <-chan zk.Event) {
CHECK:
	for {
		select {
		case connStatus := <-event:
			switch connStatus.State {
			case zk.StateDisconnected:
				//reconnect
				servers := strings.Split(pets.addr, ",")
				conn, ec, err := zk.Connect(servers, 10*time.Second)
				if err != nil {
					break CHECK
				}
				pets.conn = conn
				event = ec
				continue
			default:
				continue
			}
		case <-pets.stopCh:
			break CHECK
		}
	}
}

func (pets *Pets) CreateNode(path string, value []byte) error {
	if path == "" {
		return errors.New("path is nil")
	}
	flag := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)
	childPath := path
	var parentPath string
	paths := strings.Split(path, "/")
	for _, tmp_path := range paths[1 : len(paths)-1] {
		parentPath += "/" + tmp_path
		exist, _, err := pets.conn.Exists(parentPath)
		if err != nil {
			return err
		}
		if !exist {
			_, err = pets.conn.Create(parentPath, nil, 0, acl)
			if err != nil {
				return err
			}
		}
	}
	exist, _, err := pets.conn.Exists(childPath)
	if err != nil {
		return err
	}
	if !exist {
		_, err = pets.conn.Create(childPath, value, flag, acl)
		if err != nil {
			return err
		}
	} else {
		return errors.New(fmt.Sprintf("children path: %s  exists", childPath))
	}
	return nil
}

func (pets *Pets) SetNode(path string, value []byte) error {
	exist, stat, err := pets.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return errors.New("path does not exist, can't set value")
	}
	_, err = pets.conn.Set(path, value, stat.Version)
	return err
}

func (pets *Pets) Get(path string) ([]byte, error) {
	exist, _, err := pets.conn.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, errors.New("path does not exist, can't get")
	}
	value, _, err := pets.conn.Get(path)
	return value, err
}

func (pets *Pets) GetSelfWatcher(path string) ([]byte, <-chan zk.Event, error) {
	exist, _, err := pets.conn.Exists(path)
	if err != nil {
		return nil, nil, err
	}
	if !exist {
		return nil, nil, errors.New("path does not exist, can't get parent watcher")
	}
	value, _, event, err := pets.conn.GetW(path)
	return value, event, err
}

func (pets *Pets) GetChildren(path string) ([]string, error) {
	exist, _, err := pets.conn.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, errors.New("path does not exist, can't get children node list")
	}
	value, _, err := pets.conn.Children(path)
	return value, err
}

func (pets *Pets) GetChildrenWatcher(path string) ([]string, <-chan zk.Event, error) {
	exist, _, err := pets.conn.Exists(path)
	if err != nil {
		return nil, nil, err
	}
	if !exist {
		return nil, nil, errors.New("path does not exist, can't get children node watcher")
	}
	value, _, event, err := pets.conn.ChildrenW(path)
	return value, event, err
}

func (pets *Pets) DeleteNode(path string) error {
	exist, stats, err := pets.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		return errors.New("path does not exist, can't delete node")
	}
	return pets.conn.Delete(path, stats.Version)
}

func (pets *Pets) RegisterPet(path string, value []byte) error {
	err := pets.CreateNode(path, value)
	if err != nil {
		return err
	}
	_, ev, err := pets.GetSelfWatcher(path)
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-ev:
			pets.RegisterPet(path, value)
		}
	}()
	return nil
}

func (pets *Pets) SetNameBatch(shortName string, fullNames []string, values [][]byte) {
	pets.Lock()
	defer pets.Unlock()
	pets.shortMap[shortName] = make([]string, 0)
	if len(fullNames) == 0 || len(values) == 0 {
		return
	}
	for k, _ := range fullNames {
		pets.nameMap[fullNames[k]] = values[k]
		pets.shortMap[shortName] = append(pets.shortMap[shortName], fullNames[k])
	}
}

func (pets *Pets) UpdateNames(shortName string, fullPath string) error {
	children, ch, err := pets.GetChildrenWatcher(fullPath)
	if err != nil {
		return err
	}
	fullNames := make([]string, 0)
	nameNodes := make([][]byte, 0)
	for _, v := range children {
		full_node := fullPath + "/" + v
		data, err := pets.Get(full_node)
		if err != nil {
			continue
		}
		fullNames = append(fullNames, full_node)
		nameNodes = append(nameNodes, data)
	}
	pets.SetNameBatch(shortName, fullNames, nameNodes)
	go func() {
		select {
		case <-ch:
			pets.UpdateNames(shortName, fullPath)
		}
	}()
	return nil
}

func (pets *Pets) GetOne(shortName string) ([]byte, error) {
	if shortName == "" {
		return nil, errors.New("shortname format error")
	}
	if _, ok := pets.shortMap[shortName]; !ok {
		return nil, errors.New("shortname value doesn't exist")
	}
	idx := rand.Intn(len(pets.shortMap[shortName]))
	return pets.nameMap[pets.shortMap[shortName][idx]], nil
}

func (pets *Pets) GetAll(shortName string) ([][]byte, error) {
	if shortname == "" {
		return nil, errors.New("shortname format error")
	}
	if _, ok := pets.shortMap[shortName]; !ok {
		return nil, errors.New("shortname value doesn't exist")
	}
	batch_value := make([][]byte, 0)
	for _, fullname := range pets.shortMap[shortName] {
		batch_value = append(batch_vaue, pets.nameMap[fullname])
	}
	return batch_value, nil
}

func (pets *Pets) Close() {
	pets.stopCh <- true
	pets.conn.Close()
}
