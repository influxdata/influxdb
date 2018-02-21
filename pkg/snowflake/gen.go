package snowflake

import (
	"fmt"
	"sync"
	"time"
)

const (
	epoch        = 1491696000000
	serverBits   = 10
	sequenceBits = 12
	serverShift  = sequenceBits
	timeShift    = sequenceBits + serverBits
	serverMax    = ^(-1 << serverBits)
	sequenceMask = ^(-1 << sequenceBits)
)

type Generator struct {
	rw            sync.Mutex
	lastTimestamp uint64
	machineID     int
	sequence      int32
}

func New(machineID int) *Generator {
	if machineID < 0 || machineID > serverMax {
		panic(fmt.Errorf("invalid machine id; must be 0 ≤ id < %d", serverMax))
	}
	return &Generator{
		machineID:     machineID,
		lastTimestamp: 0,
		sequence:      0,
	}
}

func (g *Generator) MachineID() int {
	return g.machineID
}

func (g *Generator) Next() uint64 {
	t := now()
	g.rw.Lock()
	if t == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & sequenceMask
		if g.sequence == 0 {
			t = g.nextMillis()
		}
	} else if t < g.lastTimestamp {
		t = g.nextMillis()
	} else {
		g.sequence = 0
	}
	g.lastTimestamp = t
	seq := g.sequence
	g.rw.Unlock()

	tp := (t - epoch) << timeShift
	sp := uint64(g.machineID << serverShift)
	n := tp | sp | uint64(seq)

	return n
}

func (g *Generator) NextString() string {
	var s [11]byte
	encode(&s, g.Next())
	return string(s[:])
}

func (g *Generator) AppendNext(s *[11]byte) {
	encode(s, g.Next())
}

func (g *Generator) nextMillis() uint64 {
	t := now()
	for t <= g.lastTimestamp {
		time.Sleep(100 * time.Microsecond)
		t = now()
	}
	return t
}

func now() uint64 { return uint64(time.Now().UnixNano() / 1e6) }

var digits = [...]byte{
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
	'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
	'U', 'V', 'W', 'X', 'Y', 'Z', '_', 'a', 'b', 'c',
	'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
	'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
	'x', 'y', 'z', '~'}

func encode(s *[11]byte, n uint64) {
	s[10], n = digits[n&0x3f], n>>6
	s[9], n = digits[n&0x3f], n>>6
	s[8], n = digits[n&0x3f], n>>6
	s[7], n = digits[n&0x3f], n>>6
	s[6], n = digits[n&0x3f], n>>6
	s[5], n = digits[n&0x3f], n>>6
	s[4], n = digits[n&0x3f], n>>6
	s[3], n = digits[n&0x3f], n>>6
	s[2], n = digits[n&0x3f], n>>6
	s[1], n = digits[n&0x3f], n>>6
	s[0] = digits[n&0x3f]
}
