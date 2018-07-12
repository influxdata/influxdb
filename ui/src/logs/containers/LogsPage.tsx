import React, {PureComponent} from 'react'
import uuid from 'uuid'
import _ from 'lodash'
import moment from 'moment'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'

import {
  getSourceAndPopulateNamespacesAsync,
  setTimeRangeAsync,
  setTimeBounds,
  setTimeWindow,
  setTimeMarker,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
  setSearchTermAsync,
  addFilter,
  removeFilter,
  changeFilter,
  fetchMoreAsync,
  getLogConfigAsync,
  updateLogConfigAsync,
} from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import LogViewerHeader from 'src/logs/components/LogViewerHeader'
import HistogramChart from 'src/shared/components/HistogramChart'
import LogsGraphContainer from 'src/logs/components/LogsGraphContainer'
import OptionsOverlay from 'src/logs/components/OptionsOverlay'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/LogsFilterBar'
import LogsTable from 'src/logs/components/LogsTable'
import {getDeep} from 'src/utils/wrappers'
import {colorForSeverity} from 'src/logs/utils/colors'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import {
  orderTableColumns,
  filterTableColumns,
} from 'src/dashboards/utils/tableGraph'

import {SeverityFormatOptions, SECONDS_TO_MS} from 'src/logs/constants'
import {Source, Namespace} from 'src/types'

import {HistogramData, TimePeriod} from 'src/types/histogram'
import {
  Filter,
  SeverityLevel,
  SeverityFormat,
  LogsTableColumn,
  LogConfig,
  TableData,
  TimeRange,
  TimeWindow,
  TimeMarker,
  TimeBounds,
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
  setTimeBounds: (timeBounds: TimeBounds) => void
  setTimeWindow: (timeWindow: TimeWindow) => void
  setTimeMarker: (timeMarker: TimeMarker) => void
  setNamespaceAsync: (namespace: Namespace) => void
  changeZoomAsync: (timeRange: TimeRange) => void
  executeQueriesAsync: () => void
  setSearchTermAsync: (searchTerm: string) => void
  fetchMoreAsync: (queryTimeEnd: string, lastTime: number) => Promise<void>
  addFilter: (filter: Filter) => void
  removeFilter: (id: string) => void
  changeFilter: (id: string, operator: string, value: string) => void
  getConfig: (url: string) => Promise<void>
  updateConfig: (url: string, config: LogConfig) => Promise<void>
  timeRange: TimeRange
  histogramData: HistogramData
  tableData: TableData
  searchTerm: string
  filters: Filter[]
  queryCount: number
  logConfig: LogConfig
  logConfigLink: string
}

interface State {
  searchString: string
  liveUpdating: boolean
  isOverlayVisible: boolean
}

class LogsPage extends PureComponent<Props, State> {
  private interval: NodeJS.Timer

  constructor(props: Props) {
    super(props)

    this.state = {
      searchString: '',
      liveUpdating: false,
      isOverlayVisible: false,
    }
  }

  public componentDidUpdate() {
    if (!this.props.currentSource) {
      this.props.getSource(this.props.sources[0].id)
    }
  }

  public componentDidMount() {
    this.props.getSources()
    this.props.getConfig(this.logConfigLink)

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
      <>
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
              data={this.tableData}
              onScrollVertical={this.handleVerticalScroll}
              onScrolledToTop={this.handleScrollToTop}
              isScrolledToTop={liveUpdating}
              onTagSelection={this.handleTagSelection}
              fetchMore={this.props.fetchMoreAsync}
              timeRange={timeRange}
              tableColumns={this.tableColumns}
              severityFormat={this.severityFormat}
            />
          </div>
        </div>
        {this.renderImportOverlay()}
      </>
    )
  }

  private get tableData(): TableData {
    const {tableData} = this.props
    const tableColumns = this.tableColumns
    const columns = _.get(tableData, 'columns', [])
    const values = _.get(tableData, 'values', [])
    const data = [columns, ...values]

    const filteredData = filterTableColumns(data, tableColumns)
    const orderedData = orderTableColumns(filteredData, tableColumns)
    const updatedColumns: string[] = _.get(orderedData, '0', [])
    const updatedValues = _.slice(orderedData, 1)

    return {columns: updatedColumns, values: updatedValues}
  }

  private get logConfigLink(): string {
    return this.props.logConfigLink
  }

  private get tableColumns(): LogsTableColumn[] {
    const {logConfig} = this.props
    return _.get(logConfig, 'tableColumns', [])
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
        timeRange={timeRange}
        onSetTimeMarker={this.handleSetTimeMarker}
        onSetTimeWindow={this.handleSetTimeWindow}
        liveUpdating={liveUpdating && !this.isSpecificTimeRange}
        availableSources={sources}
        onChooseSource={this.handleChooseSource}
        onChooseNamespace={this.handleChooseNamespace}
        currentSource={currentSource}
        currentNamespaces={currentNamespaces}
        currentNamespace={currentNamespace}
        onChangeLiveUpdatingStatus={this.handleChangeLiveUpdatingStatus}
        onShowOptionsOverlay={this.handleToggleOverlay}
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

  private handleSetTimeBounds = async () => {
    const {seconds, windowOption, timeOption} = this.props.timeRange
    let lower = `now() - ${windowOption}`
    let upper = null

    if (timeOption !== 'now') {
      const numberTimeOption = new Date(timeOption).valueOf()
      const milliseconds = seconds * SECONDS_TO_MS
      lower = moment(numberTimeOption - milliseconds).toISOString()
      upper = moment(numberTimeOption + milliseconds).toISOString()
    }

    const timeBounds: TimeBounds = {
      lower,
      upper,
    }

    await this.props.setTimeBounds(timeBounds)

    this.props.setTimeRangeAsync(this.props.timeRange)
    this.fetchNewDataset()
  }

  private handleSetTimeWindow = async (timeWindow: TimeWindow) => {
    await this.props.setTimeWindow(timeWindow)
    this.handleSetTimeBounds()
  }

  private handleSetTimeMarker = async (timeMarker: TimeMarker) => {
    await this.props.setTimeMarker(timeMarker)
    this.handleSetTimeBounds()
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

  private handleToggleOverlay = (): void => {
    this.setState({isOverlayVisible: !this.state.isOverlayVisible})
  }

  private renderImportOverlay = (): JSX.Element => {
    const {isOverlayVisible} = this.state

    return (
      <OverlayTechnology visible={isOverlayVisible}>
        <OptionsOverlay
          severityLevels={DEFAULT_SEVERITY_LEVELS} // Todo: replace with real
          onUpdateSeverityLevels={this.handleUpdateSeverityLevels}
          onDismissOverlay={this.handleToggleOverlay}
          columns={this.tableColumns}
          onUpdateColumns={this.handleUpdateColumns}
          onUpdateSeverityFormat={this.handleUpdateSeverityFormat}
          severityFormat={this.severityFormat}
        />
      </OverlayTechnology>
    )
  }

  private handleUpdateSeverityLevels = (levels: SeverityLevel[]) => {
    // Todo: Handle saving of these new severity colors here
    levels = levels
  }

  private handleUpdateSeverityFormat = async (format: SeverityFormat) => {
    const {logConfig} = this.props
    await this.props.updateConfig(this.logConfigLink, {
      ...logConfig,
      severityFormat: format,
    })
  }

  private get severityFormat(): SeverityFormat {
    const {logConfig} = this.props
    const severityFormat = _.get(
      logConfig,
      'severityFormat',
      SeverityFormatOptions.dotText
    )
    return severityFormat
  }

  private handleUpdateColumns = async (tableColumns: LogsTableColumn[]) => {
    const {logConfig} = this.props
    await this.props.updateConfig(this.logConfigLink, {
      ...logConfig,
      tableColumns,
    })
  }
}

const mapStateToProps = ({
  sources,
  links: {
    config: {logViewer},
  },
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
    logConfig,
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
  logConfig,
  logConfigLink: logViewer,
})

const mapDispatchToProps = {
  getSource: getSourceAndPopulateNamespacesAsync,
  getSources: getSourcesAsync,
  setTimeRangeAsync,
  setTimeBounds,
  setTimeWindow,
  setTimeMarker,
  setNamespaceAsync,
  executeQueriesAsync,
  changeZoomAsync,
  setSearchTermAsync,
  addFilter,
  removeFilter,
  changeFilter,
  fetchMoreAsync,
  getConfig: getLogConfigAsync,
  updateConfig: updateLogConfigAsync,
}

export default connect(mapStateToProps, mapDispatchToProps)(LogsPage)
