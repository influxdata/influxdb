import React, {PropTypes} from 'react';
import {getDatabaseManager, createDatabase} from 'shared/apis/index';
import DatabaseManager from '../components/DatabaseManager';

const {shape, string} = PropTypes;

const DatabaseManagerApp = React.createClass({
  propTypes: {
    params: shape({
      clusterID: string.isRequired,
      database: string.isRequired,
    }).isRequired,
  },

  componentDidMount() {
    this.getData();
  },

  getInitialState() {
    return {
      databases: [],
      dbStats: {
        diskBytes: '',
        numMeasurements: 0,
        numSeries: 0,
      },
      users: [],
      queries: [],
      replicationFactors: [],
      selectedDatabase: null,
    };
  },

  getData(selectedDatabase) {
    const {clusterID, database} = this.props.params;
    getDatabaseManager(clusterID, selectedDatabase || database)
      .then(({data}) => {
        this.setState({
          databases: data.databases,
          dbStats: data.databaseStats,
          users: data.users,
          queries: data.queries || [],
          replicationFactors: data.replicationFactors,
        });
      });
  },

  handleClickDatabase(selectedDatabase) {
    this.getData(selectedDatabase);
    this.setState({selectedDatabase});
  },

  handleCreateDatabase(db) {
    createDatabase(db);
  },

  render() {
    const {databases, dbStats, queries, users, replicationFactors} = this.state;
    const {clusterID, database} = this.props.params;

    return (
      <DatabaseManager
        clusterID={clusterID}
        database={database}
        databases={databases}
        dbStats={dbStats}
        queries={queries}
        users={users}
        replicationFactors={replicationFactors}
        onClickDatabase={this.handleClickDatabase}
        onCreateDatabase={this.handleCreateDatabase}
      />
    );
  },
});

export default DatabaseManagerApp;
