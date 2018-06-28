import React, {PureComponent} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import VisHeader from 'src/data_explorer/components/VisHeader'
import VisView from 'src/data_explorer/components/VisView'

import {GRAPH, TABLE} from 'src/shared/constants'
import buildQueries from 'src/utils/buildQueriesForGraphs'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Source, Query, QueryConfig, Template, TimeRange} from 'src/types'

const META_QUERY_REGEX = /^(show|create|drop)/i

interface Props {
  source: Source
  views: string[]
  autoRefresh: number
  templates: Template[]
  timeRange: TimeRange
  queryConfigs: QueryConfig[]
  activeQueryIndex: number
  manualRefresh: number
  editQueryStatus: () => void
  errorThrown: () => void
}

interface State {
  view: string
}

@ErrorHandling
class DataExplorerVisualization extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = this.initialState
  }

  public componentWillReceiveProps(nextProps: Props) {
    const {activeQueryIndex, queryConfigs} = nextProps
    const nextQueryText = this.getQueryText(queryConfigs, activeQueryIndex)
    const queryText = this.getQueryText(
      this.props.queryConfigs,
      this.props.activeQueryIndex
    )

    if (queryText === nextQueryText) {
      return
    }

    if (nextQueryText.match(META_QUERY_REGEX)) {
      return this.setState({view: TABLE})
    }

    this.setState({view: GRAPH})
  }

  public render() {
    const {
      views,
      templates,
      autoRefresh,
      manualRefresh,
      editQueryStatus,
      errorThrown,
    } = this.props

    const {view} = this.state

    return (
      <div className="graph">
        <VisHeader
          view={view}
          views={views}
          query={this.query}
          errorThrown={errorThrown}
          onToggleView={this.handleToggleView}
        />
        <div className={this.visualizationClass}>
          <VisView
            view={view}
            query={this.query}
            templates={templates}
            queries={this.queries}
            autoRefresh={autoRefresh}
            manualRefresh={manualRefresh}
            editQueryStatus={editQueryStatus}
          />
        </div>
      </div>
    )
  }

  private get visualizationClass(): string {
    const {view} = this.state

    return classnames({
      'graph-container': view === GRAPH,
      'table-container': view === TABLE,
    })
  }

  private get queries(): Query[] {
    const {queryConfigs, timeRange} = this.props
    return buildQueries(queryConfigs, timeRange)
  }

  private get query(): Query {
    const {activeQueryIndex} = this.props
    const activeQuery = this.queries[activeQueryIndex]
    const defaultQuery = this.queries[0]
    return activeQuery || defaultQuery
  }

  private handleToggleView = (view: string): void => {
    this.setState({view})
  }

  private getQueryText(queryConfigs: QueryConfig[], index: number): string {
    // rawText can be null
    return _.get(queryConfigs, [`${index}`, 'rawText'], '') || ''
  }

  private get initialState(): {view: string} {
    const {activeQueryIndex, queryConfigs} = this.props
    const activeQueryText = this.getQueryText(queryConfigs, activeQueryIndex)

    if (activeQueryText.match(META_QUERY_REGEX)) {
      return {view: TABLE}
    }

    return {view: GRAPH}
  }
}

export default DataExplorerVisualization
