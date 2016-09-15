import React, {PropTypes} from 'react';
import _ from 'lodash';
import classNames from 'classnames';

import {showDatabases, showRetentionPolicies} from 'shared/apis/metaQuery';
import showDatabasesParser from 'shared/parsing/showDatabases';
import showRetentionPoliciesParser from 'shared/parsing/showRetentionPolicies';

const DatabaseList = React.createClass({
  propTypes: {
    query: PropTypes.shape({}).isRequired,
    onChooseNamespace: PropTypes.func.isRequired,
  },

  contextTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    clusterID: PropTypes.string.isRequired,
  },

  getInitialState() {
    return {
      namespaces: [],
    };
  },

  componentDidMount() {
    const {dataNodes, clusterID} = this.context;
    showDatabases(dataNodes, clusterID).then((resp) => {
      const {errors, databases} = showDatabasesParser(resp.data);
      if (errors.length) {
        // do something
      }

      const namespaces = [];
      showRetentionPolicies(dataNodes, databases, clusterID).then((res) => {
        res.data.results.forEach((result, index) => {
          const {errors: errs, retentionPolicies} = showRetentionPoliciesParser(result);
          if (errs.length) {
            // do something
          }

          retentionPolicies.forEach((rp) => {
            namespaces.push({
              database: databases[index],
              retentionPolicy: rp.name,
            });
          });
        });

        this.setState({namespaces});
      });
    });
  },

  render() {
    const {onChooseNamespace, query} = this.props;

    return (
      <ul className="query-editor__list">
        {this.state.namespaces.map((namespace) => {
          const {database, retentionPolicy} = namespace;
          const isActive = database === query.database && retentionPolicy === query.retentionPolicy;

          return (
            <li className={classNames('query-editor__list-item query-editor__list-radio', {active: isActive})} key={`${database}..${retentionPolicy}`} onClick={_.wrap(namespace, onChooseNamespace)}>
              {database}.{retentionPolicy}
            </li>
          );
        })}
      </ul>
    );
  },
});

export default DatabaseList;
