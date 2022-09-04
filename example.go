
package main

import "sync"
import "fmt"
import "strings"
import "log"
import "time"
import "math/rand"
import "strconv"
import "errors"

// on immutable data

// let's say we have some arbitrary tree of data
//   * we want people to be able to query it quickly and often
//   * it needs to be mutable, but we are satisfied if the mutations are serial ( no need for concurrent updates )
//   * however, the processes that people run against the tree should be against specific versions
//   * no changes should be seen to occur by readers regardless of writers making updates
//   * at the same time, readers should never stop writers from making updates
//   * for fun, let's use "Optimistic Concurrency" to avoid having to build any kind of writer queue
//   * any time two writes conflict, we just tell one of them to try again

// for languages like 'go' or 'c', you have to do this yourself.
// for languages like 'haskell' or 'erlang', it would just be part of how data fundamentally works in the language.

type ArbitraryTree struct {
	version  int
	swapLock sync.Mutex
	top      *ArbitraryTreeNode
}

type ArbitraryTreeNode struct {
	key      string
	children map[string]*ArbitraryTreeNode
	data     string
}

var ErrTryAgain = fmt.Errorf( "write conflict, please retry" )

func NewArbitraryTree() *ArbitraryTree {
	return &ArbitraryTree{
		version: 0,
		swapLock: sync.Mutex{},
		top: &ArbitraryTreeNode{
			key: "",
			children: make(map[string]*ArbitraryTreeNode),
			data: "",
		},
	}
}

func path_to_keys( path string ) ([]string, error) {
	if len( path ) == 0 {
		return nil, fmt.Errorf( "path must start with '/'" )
	}
	// strip the "" from the leading "/", since that just refers to the top node
	bits := strings.Split( path[1:], "/" )
	return bits, nil
}

func (tt *ArbitraryTree) atomicGetTop() (int, *ArbitraryTreeNode) {
	tt.swapLock.Lock()
	defer tt.swapLock.Unlock()
	
	return tt.version, tt.top
}

func (tt *ArbitraryTree) atomicUpdateTop( currentVersion int, newTop *ArbitraryTreeNode ) error {
	tt.swapLock.Lock()
	defer tt.swapLock.Unlock()
	
	if currentVersion != tt.version {
		return ErrTryAgain
	}
	
	tt.version += 1
	tt.top = newTop
	
	return nil
}

// to add the key, we atomically grab the current version, then (re)create the nodes from the top
// down to the key being added, never altering the data that is already in place
// that means that data stays safe for readers to continue using while we create the updated
//   tree structure.
// to avoid reduplicating too much of the tree, we copy in references to the existing nodes for
//   everything that isn't changing. a tree with 1,000,000 items that is at most 7 entries high
//   would only have to recreate upto 7 nodes, with the rest of them being shared.
// we could create a queue for incoming writers. in our case, if the tree is updated while we are
//   making our updated copy, we throw it away and tell the caller to try again.
// this is known as "optimistic concurrency" and is common for distributed document databases, and
//   other cases where writes can be rare. usually they protect per-document, we are protecting the
//   database in the same manner.
// 
func (tt *ArbitraryTree) AddKey( path string, value string ) (int, error) {
	
	keys, err := path_to_keys( path )
	if err != nil {
		return 0, fmt.Errorf( "failed to convert path to keys: %w", err )
	}
	
	currentVersion, readCurrent := tt.atomicGetTop()
	
	// once we construct these, we never change them again
	// that means they are safe to be shared between different
	//   versions of the tree
	// 
	newTop := &ArbitraryTreeNode{
		key: readCurrent.key,
		data: readCurrent.data,
		children: make(map[string]*ArbitraryTreeNode),
	}
	
	writeCurrent := newTop
	
	// we can just copy the children of the old node into the new node
	// we'll then replace whichever one we're updating
	// 
	for key, childNode := range readCurrent.children {
		writeCurrent.children[ key ] = childNode
	}
	
	// create new nodes down to where the changed value is
	// have them reference the same subtrees as the existing nodes
	// since subtrees never change, it's completely safe to share them
	// readers in those trees don't have to be protected
	// 
	for _, key := range keys {
		if readCurrent != nil {
			// if !ok, readCurrent will be set to nil
			readCurrent, _ = readCurrent.children[ key ]
		}
		
		if readCurrent != nil {
			nextNode := &ArbitraryTreeNode{
				key: readCurrent.key,
				data: readCurrent.data,
				children: make(map[string]*ArbitraryTreeNode),
			}
			
			for childKey, childNode := range readCurrent.children {
				nextNode.children[ childKey ] = childNode
			}
			
			writeCurrent.children[ key ] = nextNode
			writeCurrent = nextNode
		} else {
			nextNode := &ArbitraryTreeNode{
				key: key,
				data: "",
				children: make(map[string]*ArbitraryTreeNode),
			}
			writeCurrent.children[ key ] = nextNode
			writeCurrent = nextNode
		}
	}
	
	writeCurrent.data = value
	
	err = tt.atomicUpdateTop( currentVersion, newTop )
	if err != nil {
		return 0, err
	}
	
	return currentVersion, nil
}

func (tt *ArbitraryTree) GetKey( path string ) (int, string, error) {
	
	keys, err := path_to_keys( path )
	if err != nil {
		return 0, "", fmt.Errorf( "failed to convert path to keys: %w", err )
	}
	
	currentVersion, readCurrent := tt.atomicGetTop()
	
	// since the data structure is immutable, we don't have to hold any locks after getting the current top node of it
	
	for _, key := range keys {
		childNode, ok := readCurrent.children[ key ]
		if ! ok {
			return currentVersion, "", nil
		} else {
			readCurrent = childNode
		}
	}
	
	return currentVersion, readCurrent.data, nil
}

func (tt *ArbitraryTree) RemoveKey( path string ) (int,bool,error) {
	keys, err := path_to_keys( path )
	if err != nil {
		return 0, false, fmt.Errorf( "failed to convert path to keys: %w", err )
	}
	
	version, read := tt.atomicGetTop()
	
	// to remove a key, let's keep track of our path down to it
	// once there, we'll clear the data.
	// then, we'll walk back up the chain and prune off any nodes that have data="" and children={}
	// except the topmost one.
	// we put it in there to ease the release function, but ensure the release function never
	//   tries to release it via index shenannigans
	
	write := &ArbitraryTreeNode{
		key: read.key,
		data: read.data,
		children: make(map[string]*ArbitraryTreeNode),
	}
	newTop := write
	nodes := []*ArbitraryTreeNode{ write }
	
	for _, key := range keys {
		nextRead, ok := read.children[ key ]
		if ! ok {
			read  = nil
			write = nil
			break
		} else {
			write = &ArbitraryTreeNode{
				key: nextRead.key,
				data: nextRead.data,
				children: make(map[string]*ArbitraryTreeNode),
			}
			for key, child := range nextRead.children {
				write.children[ key ] = child
			}
			nodes = append( nodes, write )
			read = nextRead
		}
	}
	
	removedData := false
	if write != nil && write.data != "" {
		write.data  = ""
		removedData = true
	}
	
	// index shenannigans since golang doesn't know how to reverse a list.
	// 
	if len( nodes ) > 0 {
		for ii := len(nodes) - 1 ; ii >= 1 ; ii-- {
			if nodes[ii].data == "" && len( nodes[ii].children ) == 0 {
				delete( nodes[ii - 1].children, nodes[ii].key )
			}
		}
	}
	
	err = tt.atomicUpdateTop( version, newTop )
	if err != nil {
		return 0, false, err
	}
	
	return version, removedData, nil
}

// what if we want to do an arbitrary callback with all data on paths ending with a given subkey?
// what if that callback involves passing them into a channel to be shunted out over the network to a waiting client
// network delays etc would mean we'd have to wait for the entire tree to be walked before we could do our next write
// we can simulate this by having the callback write them and call sleep while still performing writes in the backend
// 
func( tt *ArbitraryTree) ForAllMatchingKeys( keyName string, todo func( version int, path string, data string ) ) int {
	version, top := tt.atomicGetTop()
	
	type AnnotatedNode struct {
		path string
		node *ArbitraryTreeNode
	}
	
	pending := []*AnnotatedNode{
		&AnnotatedNode{
			path: "",
			node: top,
		},
	}
	for {
		if len( pending ) == 0 {
			break
		}
		current := pending[0]
		pending = pending[1:]
		if current.node.key == keyName {
			todo( version, current.path, current.node.data )
		}
		for _, child := range current.node.children {
			pending = append( pending, &AnnotatedNode{
				path: current.path + "/" + child.key,
				node: child,
			})
		}
	}
	
	return version
}

// choose an arbitrary deepest key from the tree, requiring walking all of the branches to locate it
// you wouldn't want the tree shifting and changing under you while you searched it, no?
// 
// func( tt *ArbitraryTree) DeepestKey( ... ){
// }

// or just counting the current number of branches
// 
// func( tt *ArbitraryTree) LeafCount( ... ){
// }

// or how much data is in it?
// 
func( tt *ArbitraryTree) CountNodesWithNonEmptyData()(int, int){
	version, top := tt.atomicGetTop()
	// again, now that we have a reference, as long as we hold it
	// it acts as a snapshot of the data from that point
	
	pending := []*ArbitraryTreeNode{ top }
	count := 0
	for {
		if len( pending ) == 0 {
			break
		}
		current := pending[0]
		pending = pending[1:]
		if current.data != "" {
			count += 1
		}
		for _, vv := range current.children {
			pending = append( pending, vv )
		}
	}
	
	return version, count
}

// slow update simulates a write that takes a while,
// maybe because it syncs the tree to disk before making the update live
// 
func(tt *ArbitraryTree) SlowUpdate() (int, error) {
	version, top := tt.atomicGetTop()
	
	newTop := &ArbitraryTreeNode{
		key: top.key,
		data: top.data,
		children: make(map[string]*ArbitraryTreeNode),
	}
	
	for key, childNode := range top.children {
		newTop.children[ key ] = childNode
	}
	
	newTop.children[ "last-slow-update" ] = &ArbitraryTreeNode{
		key: "last-slow-update",
		data: "some-time-or-whatever",
		children: make(map[string]*ArbitraryTreeNode),
	}
	
	time.Sleep( 250 * time.Millisecond )
	
	err := tt.atomicUpdateTop( version, newTop )
	if err != nil {
		return 0, err
	}
	
	return version, nil
}

//
// now let's run a bunch of the above all at once and see how the data changes
//

func fanout( fanName string, funcs ...func( string ) ){
	pending := len( funcs )
	doneCh  := make(chan bool)
	
	// go is stupid here and changes a single variables value instead of binding fresh ones
	// if we were using it directly as an arg, the `go` keyword immediately evaluates arguments
	// we want to use it inside of the lambda, but if we capture it by context, the language will stupidly change it for them all
	// so we have to make our wrapping lambda take the value as an argument to rebind it, or bind it to a different name that
	// is in context of the range's block. it's an incredibly stupid design due to how go handles variables and names.
	//
	for ii, ff := range funcs {
		go func( index int, todo func( string ) ){
			todo( fanName + ":" + strconv.Itoa( index ) )
			doneCh <- true
		}(ii, ff)
	}
	
	for {
		select {
		case <- doneCh:
			pending -= 1
			if pending == 0 {
				return
			}
		}
	}
}

func sleep_for_a_bit(){
	// period := time.Duration( (rand.Intn(10) * 100) ) * time.Millisecond
	period := 5 * time.Millisecond
	time.Sleep( period )
}

var gAllowedTokenBits = "abcdefghijklmnopqrstuvwxyz-"

// these could be better
//
func random_key( mxBitsPerToken int ) string {
	token := ""
	for nBits := rand.Intn( mxBitsPerToken ) + 1 ; nBits > 0 ; nBits-- {
		token += string(gAllowedTokenBits[rand.Intn(len(gAllowedTokenBits))])
	}
	return token
}

func random_path( mxTokens int, mxBitsPerToken int) string {
	path := "/"
	
	for nTokens := rand.Intn( mxTokens ) + 1 ; nTokens > 0 ; nTokens-- {
		for nBits := rand.Intn( mxBitsPerToken ) + 1 ; nBits > 0 ; nBits-- {
			path += string(gAllowedTokenBits[rand.Intn(len(gAllowedTokenBits))])
		}
		path += "/"
	}
	
	// drop the final "/"
	return path[:len(path)-1]
}

func main(){
	
	rand.Seed( time.Now().UnixNano() )
	
	tree := NewArbitraryTree()
	
	funcs := []func( string ){
		
		// these first few were just to test that fanout was working
		
		func( fanName string ){
			for ii := 0 ; ii < 10 ; ii++ {
				log.Printf( "hello fanout=%s iteration=%d", fanName, ii )
				sleep_for_a_bit()
			}
		},
		
		func( fanName string ){
			for ii := 0 ; ii < 10 ; ii++ {
				log.Printf( "world fanout=%s iteration=%d", fanName, ii )
				sleep_for_a_bit()
			}
		},
		
		func( fanName string ){
			for ii := 0; ii < 10 ; ii++ {
				log.Printf( "random fanout=%s iteration=%d path=%s", fanName, ii, random_path(10,10) )
				sleep_for_a_bit()
			}
		},
		
		// everything from here down does stuff with the tree datastructure
		// either reading or writing to it
		// the readers never wait on the writers
		// two writers will conflict, one will win and the slow retry
		// in a 'real' server, you might want a guarantee of completion
		// here a request could try again indefinitely
		// there are various strategies you could use to order the writes
		
		func( fanName string ){
			for ii := 0; ii < 10000 ; ii++ {
				path := random_path(2,2)
				for {
					log.Printf( "adding fanout=%s iteration=%d path=%s", fanName, ii, path )
					version, err := tree.AddKey( path, strconv.Itoa( ii ) )
					if err != nil {
						if errors.Is( err, ErrTryAgain ){
							log.Printf( "adding fanout=%s iteration=%d TRY-AGAIN-RECEIVED", fanName, ii )
							continue
						} else {
							panic( "unexpected error" )
						}
					}
					log.Printf( "adding fanout=%s iteration=%d path=%s version=%d", fanName, ii, path, version )
					break
				}
				// sleep_for_a_bit()
			}
		},
		
		func( fanName string ){
			for ii := 0; ii < 3000 ; ii++ {
				path := random_path(2,2)
				log.Printf( "getting fanout=%s iteration=%d path=%s", fanName, ii, path )
				version, data, err := tree.GetKey( path )
				if err != nil { panic( "wat" ) }
				log.Printf( "getting fanout=%s iteration=%d path=%s version=%d data=%s", fanName, ii, path, version, data )
				sleep_for_a_bit()
			}
		},
		
		func( fanName string ){
			for ii := 0; ii < 3000 ; ii++ {
				path := random_path(2,2)
				log.Printf( "removing fanout=%s iteration=%d path=%s", fanName, ii, path )
				for {
					version, removedData, err := tree.RemoveKey( path )
					if err != nil {
						if errors.Is( err, ErrTryAgain ){
							log.Printf( "removing fanout=%s iteration=%d path=%s TRY-AGAIN-RECEIVED", fanName, ii )
							continue
						} else {
							panic( "unexpected error" )
						}
					}
					log.Printf( "removing fanout=%s iteration=%d path=%s version=%d removed=%t", fanName, ii, path, version, removedData )
					break
				}
				sleep_for_a_bit()
			}
		},
		
		func( fanName string ){
			for ii := 0 ; ii < 1000 ; ii++ {
				version, count := tree.CountNodesWithNonEmptyData()
				log.Printf( "counting fanout=%s iteration=%d version=%d count=%d", fanName, ii, version, count )
				sleep_for_a_bit()
			}
		},
		
		func( fanName string ){
			for ii := 0 ; ii < 10 ; ii++ {
				for {
					version, err := tree.SlowUpdate()
					if err != nil {
						if errors.Is( err, ErrTryAgain ){
							log.Printf( "slow-update fanout=%s iteration=%d TRY-AGAIN-RECEIVED", fanName, ii )
							continue
						} else {
							panic( "unexpected error" )
						}
					}
					log.Printf( "slow-update fanout=%s iteration=%d version=%d", fanName, ii, version )
					break
				}
				sleep_for_a_bit()
			}
		},
		
		// watching the output of this one will show that it continues to navigate the version of the tree
		// from when it starts, regardless of writes continuing in the background.
		// essentially, each reader gets a snapshot of the tree to work with, and all trees share all possible
		// subbranches to keep memory usage and tree copying times down.
		// 
		func( fanName string ){
			for ii := 0 ; ii < 10 ; ii++ {
				key := random_key( 2 )
				count := 0
				log.Printf( "slow-read fanout=%s iteration=%d key=%s starting", fanName, ii, key )
				version := tree.ForAllMatchingKeys( key, func( version int, path string, data string ){
					count += 1
					log.Printf(
						"slow-read fanout=%s iteration=%d key=%s version=%d path=%s found=%s",
						fanName, ii, key, version, path, data,
					)
					sleep_for_a_bit()
				})
				log.Printf( "slow-read fanout=%s iteration=%d key=%s version=%d count=%dfinished", fanName, ii, key, version, count )
				sleep_for_a_bit()
			}
		},
	}
	
	fanout(
		"fans",
		func( fanName string ){ fanout( fanName, funcs... ) },
		func( fanName string ){ fanout( fanName, funcs... ) },
		func( fanName string ){ fanout( fanName, funcs... ) },
	)
	
}
