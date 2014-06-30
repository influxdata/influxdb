package rgm

import (
    "time"
)

type qsub struct {
    ids         []int
    startTm    time.Time
    endTm      time.Time
}

func (self *qsub) NewQuerySub(ids []int, startTm time.Time, endTm time.Time) *qsub {
    self = &qsub{}
    self.ids = ids
    self.startTm = startTm
    self.endTm = endTm
    return self
}
