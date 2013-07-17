package raft

func bigEndianPutBool(b []byte, value bool) {
	if value {
		b[0] = 1
	} else {
		b[0] = 0
	}
}

func bigEndianBool(b []byte) bool {
	return !(b[0] == 0)
}