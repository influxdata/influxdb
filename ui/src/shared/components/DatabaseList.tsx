import PropTypes from 'prop-types'
import React, {PureComponent} from 'react'

import _ from 'lodash'

import {QueryConfig, Source} from 'src/types'
import {Namespace} from 'src/types/query'

import {showDatabases, showRetentionPolicies} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'src/shared/parsing/showRetentionPolicies'

import DatabaseListItem from 'src/shared/components/DatabaseListItem'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DatabaseListProps {
  query: QueryConfig
  querySource: Source
  onChooseNamespace: (namespace: Namespace) => void
  source: Source
}

interface DatabaseListState {
  namespaces: Namespace[]
}

const {shape} = PropTypes

@ErrorHandling
class DatabaseList extends PureComponent<DatabaseListProps, DatabaseListState> {
  public static contextTypes = {
    source: shape({
      links: shape({}).isRequired,
    }).isRequired,
  }

  public static defaultProps: Partial<DatabaseListProps> = {
    source: null,
  }

  constructor(props) {
    super(props)
    this.getDbRp = this.getDbRp.bind(this)
    this.handleChooseNamespace = this.handleChooseNamespace.bind(this)
    this.state = {
      namespaces: [],
    }
  }

  public componentDidMount() {
    this.getDbRp()
  }

  public componentDidUpdate({querySource: prevSource, query: prevQuery}) {
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

  public async getDbRp() {
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

      const sorted = _.sortBy(namespaces, ({database}: Namespace) =>
        database.toLowerCase()
      )

      this.setState({namespaces: sorted})
    } catch (err) {
      console.error(err)
    }
  }

  public handleChooseNamespace(namespace: Namespace) {
    return () => this.props.onChooseNamespace(namespace)
  }

  public isActive(query: QueryConfig, {database, retentionPolicy}: Namespace) {
    return (
      database === query.database && retentionPolicy === query.retentionPolicy
    )
  }

  public render() {
    return (
      <div className="query-builder--column query-builder--column-db">
        <div className="query-builder--heading">DB.RetentionPolicy</div>
        <div className="query-builder--list">
          <FancyScrollbar>
            {this.state.namespaces.map(namespace => (
              <DatabaseListItem
                isActive={this.isActive(this.props.query, namespace)}
                namespace={namespace}
                onChooseNamespace={this.handleChooseNamespace}
                key={namespace.database + namespace.retentionPolicy}
              />
            ))}
          </FancyScrollbar>
        </div>
      </div>
    )
  }
}

export default DatabaseList
