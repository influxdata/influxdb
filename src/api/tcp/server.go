package tcp

import (
	"coordinator"
)

type Server interface {
	SendErrorMessage(conn Connection, t Command_CommandType, message string) error
	GetCoordinator() coordinator.Coordinator
	HandleConnection(conn Connection)
	Authenticate(conn Connection, info *Greeting_Authentication) error
	SSLAvailable() bool
	IsForceSSLUser(name string) bool
	RemoveConnection(conn Connection)
}
