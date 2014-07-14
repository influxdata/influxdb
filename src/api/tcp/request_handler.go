package tcp

import (
	. "common"
	"fmt"

	api "api/http"

	"parser"
	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"errors"
	"protocol"
)

type HandlerCallback func(conn Connection, request *Command) (error)

type RequestHandler struct {
	Server Server
	HandlerFunctions map[Command_CommandType]HandlerCallback;
}

func NewRequestHandler(server Server) *RequestHandler {
	handler := &RequestHandler{
		Server: server,
	}
	handler.HandlerFunctions = make(map[Command_CommandType]HandlerCallback)
	handler.Setup()

	return handler
}

func (self *RequestHandler) sendErrorMessage(conn Connection, t Command_CommandType, message string) error {
	return self.Server.SendErrorMessage(conn, t, message)
}

func (self *RequestHandler) verifySeries(series []*protocol.Series) error {
	for _, s := range series {
		count := len(s.GetFields())
		if count < 1 {
			return errors.New(fmt.Sprintf("at least 1 fields required"))
		}

		for index, point := range s.GetPoints() {
			cnt := len(point.GetValues())
			if cnt < 1 {
				return errors.New(fmt.Sprintf("at least 1 fields required"))
			}

			if cnt != count {
				return errors.New(fmt.Sprintf("Fields and FiledValues are missmatched. Fields specified %d but %d at %d index", count, cnt, index))
			}
		}
	}

	return nil
}

func (self *RequestHandler) WriteSeries(conn Connection, request *Command) error {
	series := request.GetSeries().GetSeries()
	if err := self.verifySeries(series); err != nil {
		return self.sendErrorMessage(conn, Command_WRITESERIES, fmt.Sprintf("Malformed series: %s", err))
	}

	err := self.Server.GetCoordinator().WriteSeriesData(conn.GetUser(), conn.GetDatabase(), series)
	if err != nil {
		return self.sendErrorMessage(conn, Command_WRITESERIES, fmt.Sprintf("Cant insert data: %s", err))
	}

	response := &Command{
		Type: &C_WRITESERIES,
		Result: &C_OK,
		Sequence: proto.Uint32(conn.GetSequence()),
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) ChangeDatabase(conn Connection, request *Command) error {
	db := request.GetDatabase().GetName()
	if len(db) != 1 {
		return self.sendErrorMessage(conn, Command_CHANGEDATABASE, fmt.Sprintf("Cannot change database: at least requires 1 name parameter"))
	}

	// TODO: check db user permission
	conn.SetDatabase(db[0])

	response := &Command{
		Type: &C_CHANGEDATABASE,
		Result: &C_OK,
		Sequence: proto.Uint32(conn.GetSequence()),
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) CreateDatabase(conn Connection, request *Command) error {
	database := request.GetDatabase().GetName()
	if len(database) != 1 {
		return self.sendErrorMessage(conn, Command_CREATEDATABASE, fmt.Sprintf("Cannot create database: requires exactly 1 name parameter"))
	}

	result := Command_OK
	response := &Command{
		Type: &C_CREATEDATABASE,
		Sequence: proto.Uint32(conn.GetSequence()),
		Database: &Command_Database{
		},
	}
	err := self.Server.GetCoordinator().CreateDatabase(conn.GetUser(), database[0])
	if err != nil {
		result = Command_FAIL
	} else {
		response.GetDatabase().Name = append(response.GetDatabase().Name, database[0])
	}

	response.Result = &result
	return conn.WriteRequest(response)
}

func (self *RequestHandler) DropDatabase(conn Connection, request *Command) error {
	database := request.GetDatabase().GetName()
	if len(database) != 1 {
		return self.sendErrorMessage(conn, Command_CREATEDATABASE, fmt.Sprintf("Cannot create database: requires exactly 1 name parameter"))
	}

	result := Command_OK
	response := &Command{
		Type: &C_DROPDATABASE,
		Sequence: proto.Uint32(conn.GetSequence()),
		Database: &Command_Database{
		},
	}

	err := self.Server.GetCoordinator().DropDatabase(conn.GetUser(), database[0])
	if err != nil {
		result = Command_FAIL
	} else {
		response.GetDatabase().Name = append(response.GetDatabase().Name, database[0])
	}

	response.Result = &result
	return conn.WriteRequest(response)
}

func (self *RequestHandler) ListDatabase(conn Connection, request *Command) error {
	databases, err := self.Server.GetCoordinator().ListDatabases(conn.GetUser())
	if err != nil {
		return self.sendErrorMessage(conn, Command_LISTDATABASE, fmt.Sprintf("Cannot list database. Error: %s", err))
	}

	response := &Command{
		Type: &C_LISTDATABASE,
		Result: &C_OK,
		Sequence: proto.Uint32(conn.GetSequence()),
		Database: &Command_Database{
		},
	}

	for _, db := range databases {
		response.GetDatabase().Name = append(response.GetDatabase().Name, db.Name)
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) Query(conn Connection, request *Command) error {
	// TODO: choose precision
	precision := SecondPrecision

	// TODO: use configuration
	writer := NewChunkedPointsWriter(conn, precision, 500, 1000)
	seriesWriter := api.NewSeriesWriter(writer.yield)
	err := self.Server.GetCoordinator().RunQuery(conn.GetUser(), conn.GetDatabase(), string(request.GetQuery().GetQuery()), seriesWriter)

	if err != nil {
		if e, ok := err.(*parser.QueryError); ok {
			return self.sendErrorMessage(conn, Command_QUERY, fmt.Sprintf("Query Failed: %+v", e))
		}

		return self.sendErrorMessage(conn, Command_QUERY, fmt.Sprintf("Failed: %+v", err))
	}

	writer.done()
	return nil
}

func (self *RequestHandler) Ping(conn Connection, request *Command) error {
	response := &Command{
		Type: &C_PING,
		Sequence: proto.Uint32(conn.GetSequence()),
		Result: &C_OK,
	}
	return conn.WriteRequest(response)
}

func (self *RequestHandler) CloseConnection(conn Connection, request *Command) error {
	conn.Close()
	return &ConnectionError{s: "closing connection"}
}

func (self *RequestHandler) ResetConnection(conn Connection, request *Command) error {
	conn.ResetState()
	return &ConnectionResetError{s: "reset request"}
}

func (self *RequestHandler) Setup() {
	self.HandlerFunctions[Command_QUERY] = self.Query
	self.HandlerFunctions[Command_LISTDATABASE] = self.ListDatabase
	self.HandlerFunctions[Command_PING] = self.Ping
	self.HandlerFunctions[Command_CREATEDATABASE] = self.CreateDatabase
	self.HandlerFunctions[Command_CHANGEDATABASE] = self.ChangeDatabase
	self.HandlerFunctions[Command_DROPDATABASE] = self.DropDatabase
	self.HandlerFunctions[Command_CLOSE] = self.CloseConnection
	self.HandlerFunctions[Command_WRITESERIES] = self.WriteSeries
	self.HandlerFunctions[Command_RESET] = self.ResetConnection
}

func (self *RequestHandler) HandleRequest(conn Connection) error {
	request := &Command{}
	err := conn.ReadMessage(request)
	if err != nil {
		return err
	}

	if request.Type != nil {
		if _, ok := self.HandlerFunctions[*request.Type]; ok {
			return self.HandlerFunctions[*request.Type](conn, request)
		} else {
			self.sendErrorMessage(conn, Command_UNKNOWN, "Unsupported operation received")
		}
	} else {
		// Error
		self.sendErrorMessage(conn, Command_UNKNOWN, "Unsupported operation received (illegal message)")
		log.Debug("Unsupported operation received (illegal message): %+v", *request)
	}

	return nil
}
