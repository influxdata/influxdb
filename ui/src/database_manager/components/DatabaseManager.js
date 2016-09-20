import React, {PropTypes} from 'react';
import {Link} from 'react-router';

import CreateDatabase from './CreateDatabase';

const {number, string, shape, arrayOf, func} = PropTypes;

const DatabaseManager = React.createClass({
  propTypes: {
    database: string.isRequired,
    databases: arrayOf(shape({})).isRequired,
    dbStats: shape({
      diskBytes: string.isRequired,
      numMeasurements: number.isRequired,
      numSeries: number.isRequired,
    }),
    users: arrayOf(shape({
      id: number,
      name: string.isRequired,
      roles: string.isRequired,
    })).isRequired,
    queries: arrayOf(string).isRequired,
    replicationFactors: arrayOf(number).isRequired,
    onClickDatabase: func.isRequired,
    onCreateDatabase: func.isRequired,
  },

  render() {
    const {database, databases, dbStats, queries, users,
      replicationFactors, onClickDatabase, onCreateDatabase} = this.props;

    return (
      <div className="page-wrapper database-manager">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <div className="dropdown minimal-dropdown">
                <button className="dropdown-toggle" type="button" id="dropdownMenu1" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                  <span className="button-text">{database}</span>
                  <span className="caret"></span>
                </button>
                <ul className="dropdown-menu" aria-labelledby="dropdownMenu1">
                  {
                    databases.map((db) => {
                      return <li onClick={() => onClickDatabase(db.Name)} key={db.Name}><Link to={`/databases/manager/${db.Name}`}>{db.Name}</Link></li>;
                    })
                  }
                </ul>
              </div>
            </div>
            <div className="enterprise-header__right">
              <button className="btn btn-sm btn-primary" data-toggle="modal" data-target="#dbModal">Create Database</button>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-sm-12 col-md-4">
              <div className="panel panel-minimal">
                <div className="panel-heading">
                  <h2 className="panel-title">Database Stats</h2>
                </div>
                <div className="panel-body">
                  <div className="db-manager-stats">
                    <div>
                      <h4>{dbStats.diskBytes}</h4>
                      <p>On Disk</p>
                    </div>
                    <div>
                      <h4>{dbStats.numMeasurements}</h4>
                      <p>Measurements</p>
                    </div>
                    <div>
                      <h4>{dbStats.numSeries}</h4>
                      <p>Series</p>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="col-sm-12 col-md-8">
              <div className="panel panel-minimal">
                <div className="panel-heading">
                  <h2 className="panel-title">Users</h2>
                </div>
                <div className="panel-body">
                  <table className="table v-center margin-bottom-zero">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Role</th>
                      </tr>
                    </thead>
                    <tbody>
                      {
                        users.map((user) => {
                          return (
                            <tr key={user.name}>
                              <td><Link title="Manage Access" to={`/accounts/${user.name}`}>{user.name}</Link></td>
                              <td>{user.roles}</td>
                            </tr>
                          );
                        })
                      }
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

          </div>
          <div className="row">
            <div className="col-md-12">
              <div className="panel panel-minimal">
                <div className="panel-heading">
                  <h2 className="panel-title">Continuous Queries Associated</h2>
                </div>
                <div className="panel-body continuous-queries">
                  {
                    queries.length ? queries.map((query, i) => <pre key={i}><code>{query}</code></pre>) : (
                      <div className="continuous-queries__empty">
                        <img src="/assets/images/continuous-query-empty.svg" />
                        <h4>No queries to display</h4>
                      </div>
                    )
                  }
                </div>
              </div>
            </div>
          </div>
        </div>
        <CreateDatabase onCreateDatabase={onCreateDatabase} replicationFactors={replicationFactors}/>
      </div>
    );
  },
});

export default DatabaseManager;
