import React, {PropTypes} from 'react';
import _ from 'lodash';

import RetentionPoliciesHeader from '../components/RetentionPoliciesHeader';
import RetentionPoliciesList from '../components/RetentionPoliciesList';
import CreateRetentionPolicyModal from '../components/CreateRetentionPolicyModal';

import {
  showDatabases,
  showRetentionPolicies,
  showShards,
  createRetentionPolicy,
  dropShard,
} from 'shared/apis/metaQuery';
import {fetchShardDiskBytesForDatabase} from 'shared/apis/stats';
import parseShowDatabases from 'shared/parsing/showDatabases';
import parseShowRetentionPolicies from 'shared/parsing/showRetentionPolicies';
import parseShowShards from 'shared/parsing/showShards';
import {diskBytesFromShardForDatabase} from 'shared/parsing/diskBytes';

const RetentionPoliciesApp = React.createClass({
  propTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      // Simple list of databases
      databases: [],

      // A list of retention policy objects for the currently selected database
      retentionPolicies: [],

      /**
       * Disk usage/node locations for all shards across a database, keyed by shard ID.
       * e.g. if shard 10 was replicated across two data nodes:
       * {
       *   10: [
       *     {nodeID: 'localhost:8088', diskUsage: 12312414},
       *     {nodeID: 'localhost:8188', diskUsage: 12312414},
       *   ],
       *   ...
       * }
       */
      shardDiskUsage: {},

      // All shards across all databases, keyed by database and retention policy. e.g.:
      //   'telegraf..default': [
      //     <shard>,
      //     <shard>
      //   ]
      shards: {},

      selectedDatabase: null,
      isFetching: true,
    };
  },

  componentDidMount() {
    showDatabases(this.props.dataNodes, this.props.params.clusterID).then((resp) => {
      const result = parseShowDatabases(resp.data);

      if (!result.databases.length) {
        this.props.addFlashMessage({
          text: 'No databases found',
          type: 'error',
        });

        return;
      }

      const selectedDatabase = result.databases[0];

      this.setState({
        databases: result.databases,
        selectedDatabase,
      });

      this.fetchInfoForDatabase(selectedDatabase);
    }).catch((err) => {
      console.error(err); // eslint-disable-line no-console
      this.addGenericErrorMessage(err.toString());
    });
  },

  fetchInfoForDatabase(database) {
    this.setState({isFetching: true});
    Promise.all([
      this.fetchRetentionPoliciesAndShards(database),
      this.fetchDiskUsage(database),
    ]).then(([rps, shardDiskUsage]) => {
      const {retentionPolicies, shards} = rps;
      this.setState({
        shardDiskUsage,
        retentionPolicies,
        shards,
      });
    }).catch((err) => {
      console.error(err); // eslint-disable-line no-console
      this.addGenericErrorMessage(err.toString());
    }).then(() => {
      this.setState({isFetching: false});
    });
  },

  addGenericErrorMessage(errMessage) {
    const defaultMsg = 'Something went wrong! Try refreshing your browser and email support@influxdata.com if the problem persists.';
    this.props.addFlashMessage({
      text: errMessage || defaultMsg,
      type: 'error',
    });
  },

  fetchRetentionPoliciesAndShards(database) {
    const shared = {};
    return showRetentionPolicies(this.props.dataNodes, database, this.props.params.clusterID).then((resp) => {
      shared.retentionPolicies = resp.data.results.map(parseShowRetentionPolicies);
      return showShards(this.props.params.clusterID);
    }).then((resp) => {
      const shards = parseShowShards(resp.data);
      return {shards, retentionPolicies: shared.retentionPolicies[0].retentionPolicies};
    });
  },

  fetchDiskUsage(database) {
    const {dataNodes, params: {clusterID}} = this.props;
    return fetchShardDiskBytesForDatabase(dataNodes, database, clusterID).then((resp) => {
      return diskBytesFromShardForDatabase(resp.data).shardData;
    });
  },

  handleChooseDatabase(database) {
    this.setState({selectedDatabase: database, retentionPolicies: []});
    this.fetchInfoForDatabase(database);
  },

  handleCreateRetentionPolicy({rpName, duration, replicationFactor}) {
    const params = {
      database: this.state.selectedDatabase,
      host: this.props.dataNodes,
      rpName,
      duration,
      replicationFactor,
      clusterID: this.props.params.clusterID,
    };

    createRetentionPolicy(params).then(() => {
      this.props.addFlashMessage({
        text: 'Retention policy created successfully!',
        type: 'success',
      });
      this.fetchInfoForDatabase(this.state.selectedDatabase);
    }).catch((err) => {
      this.addGenericErrorMessage(err.toString());
    });
  },

  render() {
    if (this.state.isFetching) {
      return <div className="page-spinner" />;
    }

    const {selectedDatabase, shards, shardDiskUsage} = this.state;

    return (
      <div className="page-wrapper retention-policies">
        <RetentionPoliciesHeader
          databases={this.state.databases}
          selectedDatabase={selectedDatabase}
          onChooseDatabase={this.handleChooseDatabase}
        />
        <div className="container-fluid">
          <RetentionPoliciesList
            retentionPolicies={this.state.retentionPolicies}
            selectedDatabase={selectedDatabase}
            shards={shards}
            shardDiskUsage={shardDiskUsage}
            onDropShard={this.handleDropShard}
          />
        </div>
        <CreateRetentionPolicyModal onCreate={this.handleCreateRetentionPolicy} dataNodes={this.props.dataNodes} />
      </div>
    );
  },

  handleDropShard(shard) {
    const {dataNodes, params} = this.props;
    dropShard(dataNodes, shard, params.clusterID).then(() => {
      const key = `${this.state.selectedDatabase}..${shard.retentionPolicy}`;

      const shardsForRP = this.state.shards[key];
      const nextShards = _.reject(shardsForRP, (s) => s.shardId === shard.shardId);

      const shards = Object.assign({}, this.state.shards);
      shards[key] = nextShards;

      this.props.addFlashMessage({
        text: `Dropped shard ${shard.shardId}`,
        type: 'success',
      });
      this.setState({shards});
    }).catch(() => {
      this.addGenericErrorMessage();
    });
  },
});

export default RetentionPoliciesApp;
