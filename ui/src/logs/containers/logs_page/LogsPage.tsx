// Libraries
import React, {Component} from 'react'
import uuid from 'uuid'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import LogsHeader from 'src/logs/components/LogsHeader'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/logs_filter_bar/LogsFilterBar'
import OverlayTechnology from 'src/clockface/components/overlays/OverlayTechnology'
import OptionsOverlay from 'src/logs/components/options_overlay/OptionsOverlay'
import LogsTable from 'src/logs/components/logs_table/LogsTable'

// Actions
import * as logActions from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Utils
import {searchToFilters} from 'src/logs/utils/search'
import {getDeep} from 'src/utils/wrappers'
import {
  applyChangesToTableData,
  isEmptyInfiniteData,
} from 'src/logs/utils/table'

// Constants
import {NOW, DEFAULT_TAIL_CHUNK_DURATION_MS} from 'src/logs/constants'

// Types
import {Source, Links, Bucket, AppState} from 'src/types/v2'
import {
  Filter,
  LogConfig,
  SearchStatus,
  SeverityFormat,
  SeverityLevelColor,
  SeverityFormatOptions,
  LogsTableColumn,
  TableData,
  ScrollMode,
} from 'src/types/logs'

// Connected Log Config
interface TableConfigStateProps {
  links: Links
  sources: Source[]
  filters: Filter[]
  logConfig: LogConfig
  searchStatus: SearchStatus
  currentBucket: Bucket
  currentSource: Source | null
  currentBuckets: Bucket[]
}

interface DispatchTableConfigProps {
  notify: typeof notifyAction
  getConfig: typeof logActions.getLogConfigAsync
  getSources: typeof getSourcesAsync
  addFilter: typeof logActions.addFilter // TODO: update addFilters
  setConfig: typeof logActions.setConfig
  updateConfig: typeof logActions.updateLogConfigAsync
  createConfig: typeof logActions.createLogConfigAsync
  removeFilter: typeof logActions.removeFilter
  changeFilter: typeof logActions.changeFilter
  clearFilters: typeof logActions.clearFilters
  setSearchStatus: typeof logActions.setSearchStatus
  setBucketAsync: typeof logActions.setBucketAsync
  getSourceAndPopulateBuckets: typeof logActions.getSourceAndPopulateBucketsAsync
}

// Connected Table Data
interface TableDataStateProps {
  tableInfiniteData: {
    forward: TableData
    backward: TableData
  }
  currentTailUpperBound: number | undefined
  nextTailLowerBound: number | undefined
}

interface DispatchTableDataProps {
  fetchTailAsync: typeof logActions.fetchTailAsync
  flushTailBuffer: typeof logActions.flushTailBuffer
  setNextTailLowerBound: typeof logActions.setNextTailLowerBound
  clearSearchData: typeof logActions.clearSearchData
}

type StateProps = TableConfigStateProps & TableDataStateProps
type DispatchProps = DispatchTableConfigProps & DispatchTableDataProps
type Props = StateProps & DispatchProps & WithRouterProps

interface State {
  isOverlayVisible: boolean
  scrollMode: ScrollMode
}

const RELATIVE_TIME = 0

class LogsPage extends Component<Props, State> {
  private interval: number
  constructor(props: Props) {
    super(props)

    this.state = {
      isOverlayVisible: false,
      scrollMode: ScrollMode.None,
    }
  }

  public componentDidUpdate() {
    const {router} = this.props
    if (!this.props.sources || this.props.sources.length === 0) {
      return router.push(`/manage-sources?redirectPath=${location.pathname}`)
    }

    this.handleLoadingStatus()
  }

  public async componentDidMount() {
    try {
      await this.props.getSources()
      await this.setCurrentSource()
      await this.props.getConfig(this.configLink)

      if (this.props.searchStatus !== SearchStatus.SourceError) {
        this.props.setSearchStatus(SearchStatus.Loading)
        this.fetchNewDataset()
      }
    } catch (e) {
      console.error('Failed to get sources and buckets for logs')
    }
  }

  public render() {
    const {
      notify,
      filters,
      searchStatus,
      currentTailUpperBound,
      nextTailLowerBound,
    } = this.props

    return (
      <>
        <div className="page">
          {this.header}
          <div className="page-contents logs-viewer">
            <SearchBar
              onSearch={this.handleSubmitSearch}
              customTime={null}
              relativeTime={RELATIVE_TIME}
              onChooseCustomTime={this.handleChooseCustomTime}
              onChooseRelativeTime={this.handleChooseRelativeTime}
            />
            <FilterBar
              filters={filters || []}
              onDelete={this.handleFilterDelete}
              onFilterChange={this.handleFilterChange}
              onClearFilters={this.handleClearFilters}
              onUpdateTruncation={this.handleUpdateTruncation}
              isTruncated={this.isTruncated}
            />
            <LogsTable
              data={this.tableData}
              onExpand={this.handleExpandMessage}
              onScrollVertical={this.handleVerticalScroll}
              onScrolledToTop={this.handleScrollToTop}
              isScrolledToTop={false}
              isTruncated={this.isTruncated}
              onTagSelection={this.handleTagSelection}
              scrollToRow={this.tableScrollToRow}
              tableColumns={this.tableColumns}
              severityFormat={this.severityFormat}
              severityLevelColors={this.severityLevelColors}
              hasScrolled={this.hasScrolled}
              tableInfiniteData={this.props.tableInfiniteData}
              onChooseCustomTime={this.handleChooseCustomTime}
              notify={notify}
              searchStatus={searchStatus}
              upper={currentTailUpperBound || nextTailLowerBound}
              lower={nextTailLowerBound}
            />
          </div>
        </div>
        {this.configOverlay}
        {this.expandedMessageContainer}
      </>
    )
  }

  private get header(): JSX.Element {
    const {sources, currentSource, currentBuckets, currentBucket} = this.props

    return (
      <LogsHeader
        liveUpdating={this.isLiveUpdating}
        availableSources={sources}
        onChooseSource={this.handleChooseSource}
        onChooseBucket={this.handleChooseBucket}
        currentSource={currentSource}
        currentBuckets={currentBuckets}
        currentBucket={currentBucket}
        onChangeLiveUpdatingStatus={this.handleChangeLiveUpdatingStatus}
        onShowOptionsOverlay={this.handleToggleOverlay}
      />
    )
  }

  private get configOverlay(): JSX.Element {
    const {isOverlayVisible} = this.state

    return (
      <OverlayTechnology visible={isOverlayVisible}>
        <OptionsOverlay
          columns={this.tableColumns}
          severityFormat={this.severityFormat}
          severityLevelColors={this.severityLevelColors}
          onSave={this.handleSaveOptions}
          onDismissOverlay={this.handleToggleOverlay}
        />
      </OverlayTechnology>
    )
  }

  private get expandedMessageContainer(): JSX.Element {
    return (
      <div
        className="logs-viewer--expanded-message-container"
        id="expanded-message-container"
      />
    )
  }

  private get tableData(): TableData {
    const forwardData = applyChangesToTableData(
      this.props.tableInfiniteData.forward,
      this.tableColumns
    )

    const backwardData = applyChangesToTableData(
      this.props.tableInfiniteData.backward,
      this.tableColumns
    )

    const data = {
      columns: forwardData.columns,
      values: [...forwardData.values, ...backwardData.values],
    }
    return data
  }

  private handleLoadingStatus() {
    if (
      !this.isClearing &&
      !isEmptyInfiniteData(this.props.tableInfiniteData)
    ) {
      this.props.setSearchStatus(SearchStatus.Loaded)
    }
  }

  private get tableColumns(): LogsTableColumn[] {
    const {logConfig} = this.props

    return getDeep<LogsTableColumn[]>(logConfig, 'tableColumns', [])
  }

  private get severityLevelColors(): SeverityLevelColor[] {
    return getDeep<SeverityLevelColor[]>(
      this.props.logConfig,
      'severityLevelColors',
      []
    )
  }

  private get severityFormat(): SeverityFormat {
    return getDeep<SeverityFormat>(
      this.props.logConfig,
      'severityFormat',
      SeverityFormatOptions.DotText
    )
  }

  private handleSaveOptions = async (config: Partial<LogConfig>) => {
    const {logConfig} = this.props
    const updatedConfig = {
      ...logConfig,
      ...config,
    }

    if (!this.isLogConfigSaved) {
      await this.props.createConfig(this.configLink, updatedConfig)
    } else {
      await this.props.updateConfig(updatedConfig)
    }
  }

  private get isLogConfigSaved(): boolean {
    return this.props.logConfig.id !== null
  }

  private setCurrentSource = async () => {
    if (!this.props.currentSource && this.props.sources.length > 0) {
      const source =
        this.props.sources.find(src => {
          return src.default
        }) || this.props.sources[0]

      return await this.props.getSourceAndPopulateBuckets(source.links.self)
    }
  }

  private handleChangeLiveUpdatingStatus = async (): Promise<void> => {
    if (this.isLiveUpdating) {
      this.setState({scrollMode: ScrollMode.TailScrolling})
      this.clearTailInterval()
    } else {
      this.handleChooseRelativeTime(NOW)
    }
  }

  private handleSubmitSearch = async (value: string): Promise<void> => {
    searchToFilters(value)
      .reverse()
      .forEach(filter => {
        this.props.addFilter(filter)
      })

    if (this.props.searchStatus === SearchStatus.Loading) {
      this.updateTableData(SearchStatus.UpdatingFilters)
    } else {
      this.updateTableData(SearchStatus.Loading)
    }
  }

  private handleFilterDelete = (id: string): void => {
    this.props.removeFilter(id)
    this.updateTableData(SearchStatus.UpdatingFilters)
  }

  private handleFilterChange = async (
    id: string,
    operator: string,
    value: string
  ): Promise<void> => {
    this.props.changeFilter(id, operator, value)
    this.updateTableData(SearchStatus.UpdatingFilters)
  }

  private handleClearFilters = async (): Promise<void> => {
    this.props.clearFilters()
  }

  private handleChooseSource = async (sourceID: string) => {
    const source = this.props.sources.find(s => s.id === sourceID)
    await this.clearCurrentSearch(SearchStatus.UpdatingSource)
    await this.props.getSourceAndPopulateBuckets(source.links.self)
    this.fetchNewDataset()
  }

  private handleChooseBucket = async (bucket: Bucket) => {
    await this.clearCurrentSearch(SearchStatus.UpdatingBucket)
    await this.props.setBucketAsync(bucket)
    this.fetchNewDataset()
  }

  private handleUpdateTruncation = (isTruncated: boolean) => {
    const {logConfig} = this.props

    this.props.setConfig({
      ...logConfig,
      isTruncated,
    })
  }

  private get configLink(): string {
    return getDeep<string>(this.props, 'links.views', '')
  }

  private get isTruncated(): boolean {
    return this.props.logConfig.isTruncated
  }

  private get isLiveUpdating(): boolean {
    return this.state.scrollMode === ScrollMode.TailTop
  }

  private get hasScrolled(): boolean {
    switch (this.state.scrollMode) {
      case ScrollMode.TailScrolling:
      case ScrollMode.TimeSelectedScrolling:
        return true
      default:
        return false
    }
  }

  private updateTableData = async (searchStatus: SearchStatus) => {
    if (this.props.searchStatus === SearchStatus.SourceError) {
      return
    }

    await this.clearCurrentSearch(searchStatus)
    await this.fetchNewDataset()
  }

  private fetchNewDataset = async () => {
    if (this.props.searchStatus === SearchStatus.SourceError) {
      return
    }

    this.startLogsTailFetchingInterval()
  }

  private clearCurrentSearch = async (searchStatus: SearchStatus) => {
    this.clearTailInterval()
    await this.props.clearSearchData(searchStatus)
  }

  private startLogsTailFetchingInterval = () => {
    this.flushTailBuffer()
    this.clearTailInterval()

    this.props.setNextTailLowerBound(Date.now())

    this.interval = window.setInterval(
      this.handleTailFetchingInterval,
      DEFAULT_TAIL_CHUNK_DURATION_MS
    )

    this.setState({scrollMode: ScrollMode.TailTop})
  }

  private flushTailBuffer(): void {
    this.props.flushTailBuffer()
  }

  private handleTailFetchingInterval = async () => {
    if (this.isClearing) {
      return
    }

    await this.props.fetchTailAsync()
  }

  private clearTailInterval = () => {
    if (this.interval) {
      clearInterval(this.interval)
      this.interval = null
    }
  }

  private get isClearing(): boolean {
    switch (this.props.searchStatus) {
      case SearchStatus.Clearing:
      case SearchStatus.None:
        return true
    }

    return false
  }

  private handleTagSelection = (selection: {tag: string; key: string}) => {
    this.props.addFilter({
      id: uuid.v4(),
      key: selection.key,
      value: selection.tag,
      operator: '==',
    })
    this.updateTableData(SearchStatus.UpdatingFilters)
  }

  /**
   * Handle scrolling to the top and resuming logs tail
   */
  private handleScrollToTop = () => {
    if (!this.isLiveUpdating && this.shouldLiveUpdate) {
      this.startLogsTailFetchingInterval()
    }
  }

  /**
   * Handle pausing logs tail on vertical scroll
   */
  private handleVerticalScroll = () => {
    if (this.isLiveUpdating) {
      this.clearTailInterval()
    }

    let scrollMode: ScrollMode

    switch (this.state.scrollMode) {
      case ScrollMode.TailTop:
        scrollMode = ScrollMode.TailScrolling
      case ScrollMode.TimeSelected:
        scrollMode = ScrollMode.TimeSelectedScrolling
      default:
        scrollMode = ScrollMode.None
    }

    this.setState({scrollMode})
  }

  private handleExpandMessage = () => {
    this.handleVerticalScroll()
  }

  /**
   * Handle choosing a custom time
   * @param time the custom date selected
   */
  private handleChooseCustomTime = async (__: string) => {
    this.setState({scrollMode: ScrollMode.TimeSelected})
    // TODO: handle updating custom time in LogState
  }

  /**
   * Handle choosing a relative time
   * @param time the epoch time selected
   */
  private handleChooseRelativeTime = async (time: number) => {
    if (time === NOW) {
      this.startLogsTailFetchingInterval()
      this.setState({scrollMode: ScrollMode.TailTop})
    } else {
      this.updateTableData(SearchStatus.UpdatingTimeBounds)
      this.setState({scrollMode: ScrollMode.TimeSelected})
    }
    // TODO: handle updating time in LogState
  }

  private handleToggleOverlay = (): void => {
    this.setState({isOverlayVisible: !this.state.isOverlayVisible})
  }

  /**
   * Controls scroll position for new searches
   */
  private get tableScrollToRow() {
    switch (this.state.scrollMode) {
      case ScrollMode.None:
      case ScrollMode.TailScrolling:
      case ScrollMode.TimeSelectedScrolling:
        return undefined
      case ScrollMode.TailTop:
        return 0
      // Todo: handle scroll pos calc when not live
    }
  }

  /**
   * Checks if logs time is set to now
   */
  private get shouldLiveUpdate(): boolean {
    // Todo: check table time is set to now
    return true
  }
}

const mstp = ({
  sources,
  links,
  logs: {
    currentSource,
    currentBuckets,
    currentBucket,
    filters,
    logConfig,
    searchStatus,
    tableInfiniteData,
    nextTailLowerBound,
    currentTailUpperBound,
  },
}: AppState): StateProps => ({
  links,
  sources,
  filters,
  logConfig,
  searchStatus,
  currentSource,
  currentBucket,
  currentBuckets,
  tableInfiniteData,
  nextTailLowerBound,
  currentTailUpperBound,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  getSources: getSourcesAsync,
  addFilter: logActions.addFilter,
  updateConfig: logActions.updateLogConfigAsync,
  createConfig: logActions.createLogConfigAsync,
  removeFilter: logActions.removeFilter,
  changeFilter: logActions.changeFilter,
  clearFilters: logActions.clearFilters,
  getConfig: logActions.getLogConfigAsync,
  setConfig: logActions.setConfig,
  setSearchStatus: logActions.setSearchStatus,
  setBucketAsync: logActions.setBucketAsync,
  getSourceAndPopulateBuckets: logActions.getSourceAndPopulateBucketsAsync,
  fetchTailAsync: logActions.fetchTailAsync,
  flushTailBuffer: logActions.flushTailBuffer,
  setNextTailLowerBound: logActions.setNextTailLowerBound,
  clearSearchData: logActions.clearSearchData,
}

export default connect<StateProps, DispatchProps, {}>(mstp, mdtp)(
  withRouter(LogsPage)
)
