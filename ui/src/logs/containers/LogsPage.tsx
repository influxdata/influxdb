import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {
  getSourceAndPopulateNamespacesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
  setSearchTermAsync,
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
  setSearchTermAsync: (searchTerm: string) => void
  timeRange: TimeRange
  histogramData: object[]
  tableData: {
    columns: string[]
    values: string[]
  }
  searchTerm: string
}

interface State {
  searchString: string
  filters: Filter[]
  liveUpdating: boolean
}

const DUMMY_FILTERS = [
  // {
  //   id: '0',
  //   key: 'host',
  //   value: 'prod1-rsavage.local',
  //   operator: '==',
  //   enabled: true,
  // },
]

class LogsPage extends PureComponent<Props, State> {
  private interval: NodeJS.Timer

  constructor(props: Props) {
    super(props)

    this.state = {
      searchString: '',
      filters: DUMMY_FILTERS,
      liveUpdating: false,
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

    this.startUpdating()
  }

  public componentWillUnmount() {
    clearInterval(this.interval)
  }

  public render() {
    const {filters, liveUpdating} = this.state
    const {searchTerm} = this.props

    const count = getDeep(this.props, 'tableData.values.length', 0)

    return (
      <div className="page">
        {this.header}
        <div className="page-contents logs-viewer">
          <Graph>{this.chart}</Graph>
          <SearchBar
            searchString={searchTerm}
            onSearch={this.handleSubmitSearch}
          />
          <FilterBar
            numResults={count}
            filters={filters}
            onUpdateFilters={this.handleUpdateFilters}
          />
          <LogsTable
            data={this.props.tableData}
            onScrollVertical={this.handleVerticalScroll}
            onScrolledToTop={this.handleScrollToTop}
            scrolledToTop={liveUpdating}
          />
        </div>
      </div>
    )
  }

  private startUpdating = () => {
    if (this.interval) {
      clearInterval(this.interval)
    }

    this.interval = setInterval(this.handleInterval, 10000)
    this.setState({liveUpdating: true})
  }

  private handleScrollToTop = () => {
    if (!this.state.liveUpdating) {
      this.startUpdating()
    }
  }

  private handleVerticalScroll = () => {
    if (this.state.liveUpdating) {
      clearInterval(this.interval)
      this.setState({liveUpdating: false})
    }
  }

  private handleInterval = () => {
    this.props.executeQueriesAsync()
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

    const {liveUpdating} = this.state

    return (
      <LogViewerHeader
        liveUpdating={liveUpdating}
        availableSources={sources}
        timeRange={timeRange}
        onChooseSource={this.handleChooseSource}
        onChooseNamespace={this.handleChooseNamespace}
        onChooseTimerange={this.handleChooseTimerange}
        currentSource={currentSource}
        currentNamespaces={currentNamespaces}
        currentNamespace={currentNamespace}
        onChangeLiveUpdatingStatus={this.handleChangeLiveUpdatingStatus}
      />
    )
  }

  private handleChangeLiveUpdatingStatus = (): void => {
    const {liveUpdating} = this.state

    if (liveUpdating) {
      clearInterval(this.interval)
      this.setState({liveUpdating: false})
    } else {
      this.startUpdating()
    }
  }

  private handleSubmitSearch = (value: string): void => {
    this.props.setSearchTermAsync(value)
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
    searchTerm,
  },
}) => ({
  sources,
  currentSource,
  currentNamespaces,
  timeRange,
  currentNamespace,
  histogramData,
  tableData,
  searchTerm,
})

const mapDispatchToProps = {
  getSource: getSourceAndPopulateNamespacesAsync,
  getSources: getSourcesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
  setSearchTermAsync,
}

export default connect(mapStateToProps, mapDispatchToProps)(LogsPage)
