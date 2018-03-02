import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import uuid from 'uuid'

import {Source, Query} from 'src/types'
import {Namespace} from 'src/types/query'

import {showDatabases, showRetentionPolicies} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'src/shared/parsing/showRetentionPolicies'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import DatabaseListItem from 'src/shared/components/DatabaseListItem'

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

  getDbRp = async () => {
    const {source} = this.context
    const {querySource} = this.props
    const proxy = _.get(querySource, ['links', 'proxy'], source.links.proxy)

    try {
      const {data} = await showDatabases(proxy)
      const {databases} = showDatabasesParser(data)
      const rps = await showRetentionPolicies(proxy, databases)
      const namespaces = rps.data.results.reduce((acc, result, index) => {
        const {retentionPolicies} = showRetentionPoliciesParser(result)

        const dbrp = retentionPolicies.map(rp => ({
          database: databases[index],
          retentionPolicy: rp.name,
        }))

        return [...acc, ...dbrp]
      }, [])

      this.setState({namespaces})
    } catch (err) {
      console.error(err)
    }
  }

  handleChooseNamespace = (namespace: Namespace) => () => {
    this.props.onChooseNamespace(namespace)
  }

  handleSortNamespace = (a, b) => {
    const nsa = a.database.toLowerCase()
    const nsb = b.database.toLowerCase()

    if (nsa > nsb) {
      return 1
    }

    if (nsa < nsb) {
      return -1
    }

    return 0
  }

  isActive = (query: Query, {database, retentionPolicy}: Namespace) =>
    database === query.database && retentionPolicy === query.retentionPolicy

  render() {
    const namespaces = this.state.namespaces.sort(this.handleSortNamespace)

    return (
      <div className="query-builder--column query-builder--column-db">
        <div className="query-builder--heading">DB.RetentionPolicy</div>
        <div className="query-builder--list">
          <FancyScrollbar>
            {namespaces.map(namespace =>
              <DatabaseListItem
                isActive={this.isActive(this.props.query, namespace)}
                namespace={namespace}
                onChooseNamespace={this.handleChooseNamespace}
                key={uuid.v4()}
              />
            )}
          </FancyScrollbar>
        </div>
      </div>
    )
  }
}

export default DatabaseList
