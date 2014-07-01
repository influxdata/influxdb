package common

type Subscription interface {
    GetIds()    []int
    GetDb()     string
}
