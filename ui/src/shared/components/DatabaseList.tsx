import React, {Component} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import _ from 'lodash'

import {Source, Query} from 'src/types'
import {Namespace} from 'src/types/query'
import {showDatabases, showRetentionPolicies} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'src/shared/parsing/showRetentionPolicies'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'

export interface DatabaseListProps {
  query: Query
  querySource: Source
  onChooseNamespace: (namespace: Namespace) => void
  source: Source | null
}

export interface DatabaseListState {
  namespaces: Namespace[]
}

export interface DatabaseListContext {
  source: Source
}

const {shape, string} = PropTypes

class DatabaseList extends Component<DatabaseListProps, DatabaseListState> {
  state = {
    namespaces: [],
  }

  context: DatabaseListContext

  public static defaultProps: Partial<DatabaseListProps> = {
    source: null,
  }

  public static contextTypes = {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  }

  componentDidMount() {
    this.getDbRp()
  }

  componentDidUpdate({querySource: prevSource, query: prevQuery}) {
    const {querySource: nextSource, query: nextQuery} = this.props
    const differentSource = !_.isEqual(prevSource, nextSource)

    if (prevQuery.rawText === nextQuery.rawText) {
      return
    }

    const newMetaQuery =
      nextQuery.rawText && nextQuery.rawText.match(/^(create|drop)/i)

    if (differentSource || newMetaQuery) {
      setTimeout(this.getDbRp, 100)
    }
  }

  getDbRp() {
    const {source} = this.context
    const {querySource} = this.props
    const proxy =
      _.get(querySource, ['links', 'proxy'], null) || source.links.proxy

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
  }

  handleChooseNamespace = (namespace: Namespace) => () => {
    this.props.onChooseNamespace(namespace)
  }

  render() {
    const {query} = this.props
    const {namespaces} = this.state
    const sortedNamespaces = namespaces.length
      ? _.sortBy(namespaces, n => n.database.toLowerCase())
      : namespaces

    return (
      <div className="query-builder--column query-builder--column-db">
        <div className="query-builder--heading">DB.RetentionPolicy</div>
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
                  onClick={this.handleChooseNamespace(namespace)}
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
  }
}

export default DatabaseList
