import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {
  getSourceAndPopulateNamespacesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
} from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import LogViewerHeader from 'src/logs/components/LogViewerHeader'
import Graph from 'src/logs/components/LogsGraph'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/LogsFilterBar'
import LogViewerChart from 'src/logs/components/LogViewerChart'
import LogsTable from 'src/logs/components/LogsTable'
import {getDeep} from 'src/utils/wrappers'

import {Source, Namespace, TimeRange} from 'src/types'

export interface Filter {
  id: string
  key: string
  value: string
  operator: string
  enabled: boolean
}

interface Props {
  sources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace
  getSource: (sourceID: string) => void
  getSources: () => void
  setTimeRangeAsync: (timeRange: TimeRange) => void
  setNamespaceAsync: (namespace: Namespace) => void
  changeZoomAsync: (timeRange: TimeRange) => void
  executeQueriesAsync: () => void
  timeRange: TimeRange
  histogramData: object[]
  tableData: {
    columns: string[]
    values: string[]
  }
}

interface State {
  searchString: string
  filters: Filter[]
}

const DUMMY_FILTERS = [
  {
    id: '0',
    key: 'host',
    value: 'prod1-rsavage.local',
    operator: '==',
    enabled: true,
  },
]

class LogsPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchString: '',
      filters: DUMMY_FILTERS,
    }
  }

  public componentDidUpdate() {
    if (!this.props.currentSource) {
      this.props.getSource(this.props.sources[0].id)
    }
  }

  public componentDidMount() {
    this.props.getSources()

    if (this.props.currentNamespace) {
      this.props.executeQueriesAsync()
    }
  }

  public render() {
    const {searchString, filters} = this.state

    const count = getDeep(this.props, 'tableData.values.length', 0)

    return (
      <div className="page">
        {this.header}
        <div className="page-contents logs-viewer">
          <Graph>{this.chart}</Graph>
          <SearchBar
            searchString={searchString}
            onChange={this.handleSearchInputChange}
            onSearch={this.handleSubmitSearch}
          />
          <FilterBar
            numResults={count}
            filters={filters}
            onUpdateFilters={this.handleUpdateFilters}
          />
          <LogsTable data={this.props.tableData} />
        </div>
      </div>
    )
  }

  private get chart(): JSX.Element {
    const {histogramData, timeRange} = this.props
    return (
      <LogViewerChart
        timeRange={timeRange}
        data={histogramData}
        onZoom={this.handleChartZoom}
      />
    )
  }

  private get header(): JSX.Element {
    const {
      sources,
      currentSource,
      currentNamespaces,
      currentNamespace,
      timeRange,
    } = this.props

    return (
      <LogViewerHeader
        availableSources={sources}
        timeRange={timeRange}
        onChooseSource={this.handleChooseSource}
        onChooseNamespace={this.handleChooseNamespace}
        onChooseTimerange={this.handleChooseTimerange}
        currentSource={currentSource}
        currentNamespaces={currentNamespaces}
        currentNamespace={currentNamespace}
      />
    )
  }

  private handleSearchInputChange = (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    this.setState({searchString: e.target.value})
  }

  private handleSubmitSearch = (): void => {
    // do the thing
  }

  private handleUpdateFilters = (filters: Filter[]): void => {
    this.setState({filters})
  }

  private handleChooseTimerange = (timeRange: TimeRange) => {
    this.props.setTimeRangeAsync(timeRange)
    this.props.executeQueriesAsync()
  }

  private handleChooseSource = (sourceID: string) => {
    this.props.getSource(sourceID)
  }

  private handleChooseNamespace = (namespace: Namespace) => {
    this.props.setNamespaceAsync(namespace)
  }

  private handleChartZoom = (lower, upper) => {
    if (lower) {
      this.props.changeZoomAsync({lower, upper})
    }
  }
}

const mapStateToProps = ({
  sources,
  logs: {
    currentSource,
    currentNamespaces,
    timeRange,
    currentNamespace,
    histogramData,
    tableData,
  },
}) => ({
  sources,
  currentSource,
  currentNamespaces,
  timeRange,
  currentNamespace,
  histogramData,
  tableData,
})

const mapDispatchToProps = {
  getSource: getSourceAndPopulateNamespacesAsync,
  getSources: getSourcesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
}

export default connect(mapStateToProps, mapDispatchToProps)(LogsPage)
