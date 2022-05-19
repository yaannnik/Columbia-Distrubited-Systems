package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it
	//panic("implement me")
	switch message.(type) {
	case *ProposeRequest:
		return server.ProposeRequestHandler(message)
	case *ProposeResponse:
		return server.ProposeResponseHandler(message)
	case *AcceptRequest:
		return server.AcceptRequestHandler(message)
	case *AcceptResponse:
		return server.AcceptResponseHandler(message)
	case *DecideRequest:
		return server.DecideRequestHandler(message)
	default:
		server_ := server.copy()
		return []base.Node{server_}
	}
}

func (server *Server) ProposeRequestHandler(message base.Message) []base.Node {
	server_ := server.copy()

	request := message.(*ProposeRequest)

	core := base.MakeCoreMessage(server_.Address(), request.From())
	npTmp := server_.n_p
	if request.N > npTmp {
		server_.n_p = request.N
	}

	response := &ProposeResponse{
		CoreMessage: core,
		Ok:          request.N > npTmp,
		N_p:         server_.n_p,
		N_a:         server_.n_a,
		V_a:         server_.v_a,
		SessionId:   request.SessionId,
	}
	server_.SetSingleResponse(response)

	nextServers := []base.Node{server_}
	return nextServers
}

func (server *Server) ProposeResponseHandler(message base.Message) []base.Node {
	server_ := server.copy()

	if server.proposer.Phase != Propose {
		return []base.Node{server_}
	}

	response := message.(*ProposeResponse)
	sender := -1
	for i, peer := range server.peers {
		if response.From() == peer {
			sender = i
			break
		}
	}
	if sender == -1 || server.proposer.Responses[sender] ||
		response.SessionId != server.proposer.SessionId {
		return []base.Node{server_}
	}

	server_.proposer.Responses[sender] = true
	server_.proposer.ResponseCount++

	if response.Ok {
		server_.proposer.SuccessCount++
		if response.N_a > server_.proposer.N_a_max {
			server_.proposer.N_a_max = response.N_a
			server_.proposer.V = response.V_a
		}
	} else {
		if server_.n_p < response.N_p {
			server_.n_p = response.N_p
		}
	}

	all, majority := len(server_.peers), len(server_.peers)/2+1
	server__ := server_.copy()

	// A node enters Accept phase after receiving majority Propose-OK
	if server_.proposer.SuccessCount >= majority {
		server_.StartAccept()
	}

	nextServers := []base.Node{server_}
	// Another node continues to be Propose phase and wait for the all response.
	if server_.proposer.ResponseCount < all {
		nextServers = append(nextServers, server__)
	}

	return nextServers
}

func (server *Server) AcceptRequestHandler(message base.Message) []base.Node {
	server_ := server.copy()

	request := message.(*AcceptRequest)

	core := base.MakeCoreMessage(server_.Address(), request.From())
	npTmp := server_.n_p
	if request.N >= npTmp {
		server_.n_p = request.N
		server_.n_a = request.N
		server_.v_a = request.V
	}

	response := &AcceptResponse{
		CoreMessage: core,
		Ok:          request.N >= npTmp,
		N_p:         server_.n_p,
		SessionId:   request.SessionId,
	}
	server_.SetSingleResponse(response)

	nextServers := []base.Node{server_}
	return nextServers
}

func (server *Server) AcceptResponseHandler(message base.Message) []base.Node {
	server_ := server.copy()

	if server.proposer.Phase != Accept {
		return []base.Node{server_}
	}

	response := message.(*AcceptResponse)
	sender := -1
	for i, peer := range server.peers {
		if response.From() == peer {
			sender = i
			break
		}
	}
	if sender == -1 || server.proposer.Responses[sender] ||
		response.SessionId != server.proposer.SessionId {
		return []base.Node{server_}
	}

	server_.proposer.Responses[sender] = true
	server_.proposer.ResponseCount++

	if response.Ok {
		server_.proposer.SuccessCount++
	} else {
		if server_.n_p < response.N_p {
			server_.n_p = response.N_p
		}
	}

	all, majority := len(server_.peers), len(server_.peers)/2+1
	server__ := server_.copy()

	// A node enters Accept phase after receiving majority Accept-OK
	if server_.proposer.SuccessCount >= majority {
		server_.StartDecide()
	}

	nextServers := []base.Node{server_}
	// Another node continues to be Propose phase and wait for the all response.
	if server_.proposer.ResponseCount < all {
		nextServers = append(nextServers, server__)
	}

	return nextServers
}

func (server *Server) DecideRequestHandler(message base.Message) []base.Node {
	server_ := server.copy()

	request := message.(*DecideRequest)

	server_.agreedValue = request.V

	core := base.MakeCoreMessage(server_.Address(), request.From())
	response := &DecideResponse{
		CoreMessage: core,
		Ok:          true,
		SessionId:   request.SessionId,
	}
	server_.SetSingleResponse(response)

	nextServers := []base.Node{server_}

	return nextServers
}

// StartPropose To start a new round of Paxos.
func (server *Server) StartPropose() {
	//TODO: implement it
	//panic("implement me")

	if base.IsNil(server.proposer.InitialValue) {
		return
	}

	server.proposer = Proposer{
		N:             server.n_p + 1,
		Phase:         Propose,
		N_a_max:       0,
		V:             server.proposer.InitialValue,
		SuccessCount:  0,
		ResponseCount: 0,
		Responses:     make([]bool, len(server.peers)),
		SessionId:     server.proposer.SessionId + 1,
		InitialValue:  server.proposer.InitialValue,
	}

	requests := []base.Message{}
	for _, peer := range server.peers {
		core := base.MakeCoreMessage(server.Address(), peer)
		request := &ProposeRequest{
			CoreMessage: core,
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId,
		}
		requests = append(requests, request)
	}
	server.SetResponse(requests)
}

func (server *Server) StartAccept() {
	if base.IsNil(server.proposer.V) {
		return
	}

	server.proposer.Phase = Accept
	server.proposer.ResponseCount = 0
	server.proposer.SuccessCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))

	requests := []base.Message{}

	for _, peer := range server.peers {
		core := base.MakeCoreMessage(server.Address(), peer)
		request := &AcceptRequest{
			CoreMessage: core,
			N:           server.proposer.N,
			V:           server.proposer.V,
			SessionId:   server.proposer.SessionId,
		}
		requests = append(requests, request)
	}

	server.SetResponse(requests)
}

func (server *Server) StartDecide() {
	if base.IsNil(server.proposer.V) {
		return
	}

	server.proposer.Phase = Decide
	server.proposer.ResponseCount = 0
	server.proposer.SuccessCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))

	requests := []base.Message{}

	for _, peer := range server.peers {
		core := base.MakeCoreMessage(server.Address(), peer)
		request := &DecideRequest{
			CoreMessage: core,
			V:           server.proposer.V,
			SessionId:   server.proposer.SessionId,
		}
		requests = append(requests, request)
	}

	server.SetResponse(requests)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// TriggerTimer A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
