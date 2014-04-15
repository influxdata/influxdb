package integration

import . "launchpad.net/gocheck"

type EngineSuite struct {
}

var _ = Suite(&EngineSuite{})

// func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidArgument(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "invalid argument foobar to the time function")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select count(*) from foo group by time(foobar) order asc", c, err)
// }

// func (self *EngineSuite) TestPercentileQueryWithInvalidNumberOfArguments(c *C) {
// 	err := common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select percentile(95) from foo group by time(1m) order asc", c, err)
// }

// func (self *EngineSuite) TestPercentileQueryWithNonNumericArguments(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select percentile(column_one, a95) from foo group by time(1m) order asc", c, err)
// }

// func (self *EngineSuite) TestPercentileQueryWithOutOfBoundNumericArguments(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select percentile(column_one, 0) from foo group by time(1m) order asc", c, err)
// 	runQueryRunError(engine, "select percentile(column_one, 105) from foo group by time(1m) order asc", c, err)
// }

// func (self *DataTestSuite) BasicQueryError(c *C) {
// 	// create an engine and assert the engine works as a passthrough if
// 	// the query only returns the raw data
// 	engine := createEngine(c, "[]")
// 	engine.coordinator.(*MockCoordinator).returnedError = fmt.Errorf("some error")
// 	err := engine.coordinator.RunQuery(nil, "", "select * from foo", func(series *protocol.Series) error {
// 		return nil
// 	})

// 	c.Assert(err, ErrorMatches, "some error")
// }

// func (self *DataTestSuite) CountQueryWithGroupByTimeInvalidNumberOfArguments(c *C) (Fun, Fun) {
// 	err := common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
// createEngine(client, c, `[]`)
// 	runQueryRunError(engine, "select count(*) from foo group by time(1h, 1m) order asc", c, err)
// }

// func (self *DataTestSuite) CountQueryWithInvalidWildcardArgument(c *C) (Fun, Fun) {
// 	return func(client Client) {
// 			createEngine(client, c, `
// [
//   {
//     "points": [
//       {
//         "values": [
//           {
//             "int64_value": 100
//           }
//         ],
//         "timestamp": 1381346641000000
//       }
//     ],
//     "name": "foo",
//     "fields": ["column_one"]
//   }
// ]
// `)
// 		}, func(client Client) {
// 			query := "select count(*) from foo group by time(1h) order asc"
// 			body, code := self.server.GetErrorBody("test_db", query, "root", "root", false, c)
// 			c.Assert(code, Equals, http.StatusBadRequest)
// 			c.Assert(body, Matches, ".*count.*")
// 		}
// }
