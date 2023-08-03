package raft

import (
	"time"
)

type Role int
type MsgType int

const (
	Leader    Role = 1
	Follower  Role = 2
	Candidate Role = 3
)

const (
	HeartBeatCheck   MsgType = 1
	LogEntriesUpdate MsgType = 2
)

const (
	SendHeartBeatsInterval = time.Millisecond * 120
	BatchSize              = 100
)
