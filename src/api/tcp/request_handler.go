package tcp

import (
	. "common"
	"fmt"

	api "api/http"

	"parser"
	"code.google.com/p/goprotobuf/proto"
	"errors"
	log "code.google.com/p/log4go"

)

type RequestHandler struct {
	Dummy int
	Server *Server
}

func (self *RequestHandler) sendErrorMessage(conn *Connection, t Command_CommandType, message string) error {
	return self.Server.SendErrorMessage(conn, t, message)
}

func (self *RequestHandler) WriteSeries(conn *Connection, request *Command) error {
	series := request.GetSeries()
	err := self.Server.Coordinator.WriteSeriesData(conn.User, conn.Database, series.GetSeries())
	if err != nil {
		self.sendErrorMessage(conn, Command_WRITESERIES, fmt.Sprintf("Cant insert data: %s", err))
		return err
	}

	v := Command_WRITESERIES
	result := Command_OK
	response := &Command{
		Type: &v,
		Result: &result,
		Sequence: proto.Uint32(conn.Sequence),
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) ChangeDatabase(conn *Connection, request *Command) error {
	db := request.GetDatabase().GetName()
	if len(db) != 1 {
		self.sendErrorMessage(conn, Command_CHANGEDATABASE, fmt.Sprintf("Cannot change database: at least requires 1 name parameter"))
		return errors.New(fmt.Sprintf("Cannot change database: at least requires 1 name parameter"))
	}

	// TODO: check db user permission
	conn.Database = db[0]

	v := Command_CHANGEDATABASE
	result := Command_OK
	response := &Command{
		Type: &v,
		Result: &result,
		Sequence: proto.Uint32(conn.Sequence),
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) CreateDatabase(conn *Connection, request *Command) error {
	database := request.GetDatabase().GetName()
	if len(database) < 1 {
		self.sendErrorMessage(conn, Command_CREATEDATABASE, fmt.Sprintf("Cannot create database: at least requires 1 name parameter"))
		return errors.New(fmt.Sprintf("Cannot create database: at least requires 1 name parameter"))
	}

	v := Command_CREATEDATABASE
	result := Command_OK
	response := &Command{
		Type: &v,
		Sequence: proto.Uint32(conn.Sequence),
		Database: &Command_Database{
		},
	}
	for _, name := range database {
		err := self.Server.Coordinator.CreateDatabase(conn.User, name)
		if err != nil {
			result = Command_SOFTFAIL
			continue
		} else {
			response.GetDatabase().Name = append(response.GetDatabase().Name, name)
		}
	}

	response.Result = &result
	return conn.WriteRequest(response)
}

func (self *RequestHandler) DropDatabase(conn *Connection, request *Command) error {
	databases := request.GetDatabase().GetName()

	v := Command_DROPDATABASE
	result := Command_OK
	response := &Command{
		Type: &v,
		Sequence: proto.Uint32(conn.Sequence),
		Database: &Command_Database{
		},
	}
	for _, name := range databases {
		err := self.Server.Coordinator.DropDatabase(conn.User, name)
		if err != nil {
			result = Command_SOFTFAIL
			continue
		} else {
			response.GetDatabase().Name = append(response.GetDatabase().Name, name)
		}
	}
	response.Result = &result
	return conn.WriteRequest(response)
}

func (self *RequestHandler) ListDatabase(conn *Connection, request *Command) error {
	databases, err := self.Server.Coordinator.ListDatabases(conn.User)

	if err != nil {
		self.sendErrorMessage(conn, Command_LISTDATABASE, fmt.Sprintf("Cannot list database. Error: %s", err))
		return err
	}

	v := Command_LISTDATABASE
	result := Command_OK
	response := &Command{
		Type: &v,
		Result: &result,
		Sequence: proto.Uint32(conn.Sequence),
		Database: &Command_Database{
		},
	}

	for _, db := range databases {
		response.GetDatabase().Name = append(response.GetDatabase().Name, db.Name)
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) Query(conn *Connection, request *Command) error {
	precision := SecondPrecision
	writer := NewChunkedPointsWriter(conn, precision, 500, 1000)

	seriesWriter := api.NewSeriesWriter(writer.yield)
	err := self.Server.Coordinator.RunQuery(conn.User, conn.Database, string(request.GetQuery().GetQuery()), seriesWriter)

	if err != nil {
		if e, ok := err.(*parser.QueryError); ok {
			self.sendErrorMessage(conn, Command_QUERY, fmt.Sprintf("Query Failed: %+v", e))
			return nil
		}

		self.sendErrorMessage(conn, Command_QUERY, fmt.Sprintf("Failed: %+v", err))
		return nil
	}

	writer.done()
	return nil
}

func (self *RequestHandler) Ping(conn *Connection, request *Command) error {
	v := Command_PING
	result := Command_OK
	response := &Command{
		Type: &v,
		Sequence: proto.Uint32(conn.Sequence),
		Result: &result,
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) CloseConnection(conn *Connection, request *Command) error {
	conn.Close()
	return &ConnectionError{s: "closing connection"}
}

func (self *RequestHandler) ResetConnection(conn *Connection, request *Command) error {
	conn.ResetState()
	return &ConnectionResetError{s: "reset request"}
}

func (self *RequestHandler) HandleRequest(conn *Connection) error {
	request := &Command{}
	err := conn.ReadMessage(request)
	if err != nil {
		return err
	}

	if request.Type != nil {
		switch (*request.Type) {
		case Command_QUERY:
			return self.Query(conn, request)
			break
		case Command_LISTDATABASE:
			return self.ListDatabase(conn, request)
			break
		case Command_PING:
			return self.Ping(conn, request)
			break
		case Command_CREATEDATABASE:
			return self.CreateDatabase(conn, request)
			break
		case Command_CHANGEDATABASE:
			return self.ChangeDatabase(conn, request)
			break
		case Command_DROPDATABASE:
			return self.DropDatabase(conn, request)
			break
		case Command_CLOSE:
			return self.CloseConnection(conn, request)
			break
		case Command_WRITESERIES:
			return self.WriteSeries(conn, request)
			break
		case Command_RESET:
			return self.ResetConnection(conn, request)
			break
		default:
			self.sendErrorMessage(conn, Command_UNKNOWN, "Unsupported operation received")
		}
	} else {
		// Not Supported Command
		self.sendErrorMessage(conn, Command_UNKNOWN, "Unsupported operation received (illegal message)")
		log.Debug("Unsupported operation received (illegal message): %+v", *request)
	}

	return nil
}
