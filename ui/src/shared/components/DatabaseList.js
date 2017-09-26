import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {showDatabases, showRetentionPolicies} from 'shared/apis/metaQuery'
import showDatabasesParser from 'shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'shared/parsing/showRetentionPolicies'

import FancyScrollbar from 'shared/components/FancyScrollbar'

const {func, shape, string} = PropTypes

const DatabaseList = React.createClass({
  propTypes: {
    query: shape({}).isRequired,
    onChooseNamespace: func.isRequired,
  },

  contextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      namespaces: [],
    }
  },

  componentDidMount() {
    const {source} = this.context
    const proxy = source.links.proxy
    showDatabases(proxy).then(resp => {
      const {errors, databases} = showDatabasesParser(resp.data)
      if (errors.length) {
        // do something
      }

      const namespaces = []
      showRetentionPolicies(proxy, databases).then(res => {
        res.data.results.forEach((result, index) => {
          const {errors: errs, retentionPolicies} = showRetentionPoliciesParser(
            result
          )
          if (errs.length) {
            // do something
          }

          retentionPolicies.forEach(rp => {
            namespaces.push({
              database: databases[index],
              retentionPolicy: rp.name,
            })
          })
        })

        this.setState({namespaces})
      })
    })
  },

  render() {
    const {query, onChooseNamespace} = this.props
    const {namespaces} = this.state
    const sortedNamespaces = namespaces.length
      ? _.sortBy(namespaces, n => n.database.toLowerCase())
      : namespaces

    return (
      <div className="query-builder--column query-builder--column-db">
        <div className="query-builder--heading">Databases</div>
        <div className="query-builder--list">
          <FancyScrollbar>
            {sortedNamespaces.map(namespace => {
              const {database, retentionPolicy} = namespace
              const isActive =
                database === query.database &&
                retentionPolicy === query.retentionPolicy

              return (
                <div
                  className={classnames('query-builder--list-item', {
                    active: isActive,
                  })}
                  key={`${database}..${retentionPolicy}`}
                  onClick={_.wrap(namespace, onChooseNamespace)}
                  data-test={`query-builder-list-item-database-${database}`}
                >
                  {database}.{retentionPolicy}
                </div>
              )
            })}
          </FancyScrollbar>
        </div>
      </div>
    )
  },
})

export default DatabaseList
