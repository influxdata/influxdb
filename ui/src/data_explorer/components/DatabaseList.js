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
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      namespaces: [],
    };
  },

  componentDidMount() {
    const {source} = this.context;
    const proxy = source.links.proxy;
    showDatabases(proxy).then((resp) => {
      const {errors, databases} = showDatabasesParser(resp.data);
      if (errors.length) {
        // do something
      }

      const namespaces = [];
      showRetentionPolicies(proxy, databases).then((res) => {
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
      <div className="query-builder--column">
        <div className="query-builder--column-heading">Databases</div>
        <ul className="qeditor--list">
          {this.state.namespaces.map((namespace) => {
            const {database, retentionPolicy} = namespace;
            const isActive = database === query.database && retentionPolicy === query.retentionPolicy;

            return (
              <li className={classNames('qeditor--list-item qeditor--list-radio', {active: isActive})} key={`${database}..${retentionPolicy}`} onClick={_.wrap(namespace, onChooseNamespace)}>
                {database}.{retentionPolicy}
              </li>
            );
          })}
        </ul>
      </div>
    );
  },
});

export default DatabaseList;
