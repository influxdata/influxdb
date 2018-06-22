import React, {PureComponent} from 'react'
import uuid from 'uuid'
import _ from 'lodash'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'

import {
  getSourceAndPopulateNamespacesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
  setSearchTermAsync,
  addFilter,
  removeFilter,
  changeFilter,
  fetchMoreAsync,
} from 'src/logs/actions'
import {
  showOverlay as showOverlayAction,
  ShowOverlay,
} from 'src/shared/actions/overlayTechnology'
import {getSourcesAsync} from 'src/shared/actions/sources'
import LogViewerHeader from 'src/logs/components/LogViewerHeader'
import HistogramChart from 'src/shared/components/HistogramChart'
import LogsGraphContainer from 'src/logs/components/LogsGraphContainer'
import OptionsOverlay from 'src/logs/components/OptionsOverlay'
import Graph from 'src/logs/components/LogsGraph'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/LogsFilterBar'
import LogsTable from 'src/logs/components/LogsTable'
import {getDeep} from 'src/utils/wrappers'
import {colorForSeverity} from 'src/logs/utils/colors'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'

import {Source, Namespace, TimeRange} from 'src/types'

import {HistogramData, TimePeriod} from 'src/types/histogram'
import {
  Filter,
  SeverityLevel,
  SeverityFormat,
  LogsTableColumn,
} from 'src/types/logs'

// Mock
import {DEFAULT_SEVERITY_LEVELS} from 'src/logs/constants'

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
  fetchMoreAsync: (queryTimeEnd: string, lastTime: number) => Promise<void>
  addFilter: (filter: Filter) => void
  removeFilter: (id: string) => void
  changeFilter: (id: string, operator: string, value: string) => void
  timeRange: TimeRange
  histogramData: HistogramData
  tableData: {
    columns: string[]
    values: string[]
  }
  searchTerm: string
  filters: Filter[]
  queryCount: number
  showOverlay: ShowOverlay
}

interface State {
  searchString: string
  liveUpdating: boolean
}

class LogsPage extends PureComponent<Props, State> {
  private interval: NodeJS.Timer

  constructor(props: Props) {
    super(props)

    this.state = {
      searchString: '',
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
      this.fetchNewDataset()
    }

    this.startUpdating()
  }

  public componentWillUnmount() {
    clearInterval(this.interval)
  }

  public render() {
    const {liveUpdating} = this.state
    const {searchTerm, filters, queryCount, timeRange} = this.props

    return (
      <div className="page">
        {this.header}
        <div className="page-contents logs-viewer">
          <LogsGraphContainer>{this.chart}</LogsGraphContainer>
          <SearchBar
            searchString={searchTerm}
            onSearch={this.handleSubmitSearch}
          />
          <FilterBar
            numResults={this.histogramTotal}
            filters={filters || []}
            onDelete={this.handleFilterDelete}
            onFilterChange={this.handleFilterChange}
            queryCount={queryCount}
          />
          <LogsTable
            count={this.histogramTotal}
            data={this.props.tableData}
            onScrollVertical={this.handleVerticalScroll}
            onScrolledToTop={this.handleScrollToTop}
            isScrolledToTop={liveUpdating}
            onTagSelection={this.handleTagSelection}
            fetchMore={this.props.fetchMoreAsync}
            timeRange={timeRange}
          />
        </div>
      </div>
    )
  }

  private get isSpecificTimeRange(): boolean {
    return !!getDeep(this.props, 'timeRange.upper', false)
  }

  private startUpdating = () => {
    if (this.interval) {
      clearInterval(this.interval)
    }

    if (!this.isSpecificTimeRange) {
      this.interval = setInterval(this.handleInterval, 10000)
      this.setState({liveUpdating: true})
    }
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

  private handleTagSelection = (selection: {tag: string; key: string}) => {
    // Do something with the tag
    this.props.addFilter({
      id: uuid.v4(),
      key: selection.key,
      value: selection.tag,
      operator: '==',
    })
    this.fetchNewDataset()
  }

  private handleInterval = () => {
    this.fetchNewDataset()
  }

  private get histogramTotal(): number {
    const {histogramData} = this.props

    return _.sumBy(histogramData, 'value')
  }

  private get chart(): JSX.Element {
    const {histogramData} = this.props

    return (
      <AutoSizer>
        {({width, height}) => (
          <HistogramChart
            data={histogramData}
            width={width}
            height={height}
            colorScale={colorForSeverity}
            onZoom={this.handleChartZoom}
          />
        )}
      </AutoSizer>
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
        liveUpdating={liveUpdating && !this.isSpecificTimeRange}
        availableSources={sources}
        timeRange={timeRange}
        onChooseSource={this.handleChooseSource}
        onChooseNamespace={this.handleChooseNamespace}
        onChooseTimerange={this.handleChooseTimerange}
        currentSource={currentSource}
        currentNamespaces={currentNamespaces}
        currentNamespace={currentNamespace}
        onChangeLiveUpdatingStatus={this.handleChangeLiveUpdatingStatus}
        onShowOptionsOverlay={this.handleShowOptionsOverlay}
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
    this.setState({liveUpdating: true})
  }

  private handleFilterDelete = (id: string): void => {
    this.props.removeFilter(id)
    this.fetchNewDataset()
  }

  private handleFilterChange = (
    id: string,
    operator: string,
    value: string
  ) => {
    this.props.changeFilter(id, operator, value)
    this.fetchNewDataset()
    this.props.executeQueriesAsync()
  }

  private handleChooseTimerange = (timeRange: TimeRange) => {
    this.props.setTimeRangeAsync(timeRange)
    this.fetchNewDataset()
  }

  private handleChooseSource = (sourceID: string) => {
    this.props.getSource(sourceID)
  }

  private handleChooseNamespace = (namespace: Namespace) => {
    this.props.setNamespaceAsync(namespace)
  }

  private handleChartZoom = (t: TimePeriod) => {
    const {start, end} = t
    const timeRange = {
      lower: new Date(start).toISOString(),
      upper: new Date(end).toISOString(),
    }

    this.props.changeZoomAsync(timeRange)
    this.setState({liveUpdating: true})
  }

  private fetchNewDataset() {
    this.props.executeQueriesAsync()
    this.setState({liveUpdating: true})
  }

  private handleShowOptionsOverlay = (): void => {
    const {showOverlay} = this.props
    const options = {
      dismissOnClickOutside: false,
      dismissOnEscape: false,
    }

    showOverlay(
      <OverlayContext.Consumer>
        {({onDismissOverlay}) => (
          <OptionsOverlay
            severityLevels={DEFAULT_SEVERITY_LEVELS} // Todo: replace with real
            onUpdateSeverityLevels={this.handleUpdateSeverityLevels}
            onDismissOverlay={onDismissOverlay}
            columns={this.fakeColumns}
            onUpdateColumns={this.handleUpdateColumns}
            onUpdateSeverityFormat={this.handleUpdateSeverityFormat}
            severityFormat="dotText" // Todo: repleace with real value
          />
        )}
      </OverlayContext.Consumer>,
      options
    )
  }

  private handleUpdateSeverityLevels = (levels: SeverityLevel[]) => {
    console.log(levels)
    // Todo: Handle saving of these new severity colors here
  }

  private handleUpdateSeverityFormat = (format: SeverityFormat) => {
    console.log(format)
    // Todo: Handle saving of the new format here
  }

  private get fakeColumns(): LogsTableColumn[] {
    const {
      tableData: {columns},
    } = this.props

    return columns.map(c => ({internalName: c, displayName: '', visible: true}))
  }

  private handleUpdateColumns = (columns: LogsTableColumn[]) => {
    console.log(columns)
    // Todo: Handle saving of column names, ordering, and visibility
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
    filters,
    queryCount,
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
  filters,
  queryCount,
})

const mapDispatchToProps = {
  getSource: getSourceAndPopulateNamespacesAsync,
  getSources: getSourcesAsync,
  showOverlay: showOverlayAction,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
  setSearchTermAsync,
  addFilter,
  removeFilter,
  changeFilter,
  fetchMoreAsync,
}

export default connect(mapStateToProps, mapDispatchToProps)(LogsPage)
