package data

type HandlerWriter interface {
	Write()
}

type localWriter struct{}

func (w localWriter) Write() {

}

type remoteWriter struct{}

func (w remoteWriter) Write() {

}
