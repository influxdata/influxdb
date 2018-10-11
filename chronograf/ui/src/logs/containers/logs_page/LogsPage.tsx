// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter, InjectedRouter} from 'react-router'

// Components
import LogsHeader from 'src/logs/components/LogsHeader'
import LoadingStatus from 'src/logs/components/loading_status/LoadingStatus'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/logs_filter_bar/LogsFilterBar'

// Actions
import * as logActions from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Utils
import {searchToFilters} from 'src/logs/utils/search'

// Constants
import {NOW} from 'src/logs/constants'

// Types
import {Source, Links, Bucket} from 'src/types/v2'
import {Filter, LogConfig, SearchStatus} from 'src/types/logs'

interface StateProps {
  links: Links
  sources: Source[]
  filters: Filter[]
  logConfig: LogConfig
  searchStatus: SearchStatus
  currentBucket: Bucket
  currentSource: Source | null
  currentBuckets: Bucket[]
}

interface PassedProps {
  router: InjectedRouter
}

interface DispatchProps {
  notify: typeof notifyAction
  getConfig: typeof logActions.getLogConfigAsync
  getSources: typeof getSourcesAsync
  addFilter: typeof logActions.addFilter
  updateConfig: typeof logActions.setConfig
  removeFilter: typeof logActions.removeFilter
  changeFilter: typeof logActions.changeFilter
  clearFilters: typeof logActions.clearFilters
  setSearchStatus: typeof logActions.setSearchStatus
  setBucketAsync: typeof logActions.setBucketAsync
  getSourceAndPopulateBuckets: typeof logActions.getSourceAndPopulateBucketsAsync
}

type Props = StateProps & PassedProps & DispatchProps

interface State {
  liveUpdating: boolean
}

const RELATIVE_TIME = 0

class LogsPage extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      liveUpdating: false,
    }
  }

  public async componentDidUpdate() {
    const {router} = this.props
    if (!this.props.sources || this.props.sources.length === 0) {
      return router.push(`/manage-sources?redirectPath=${location.pathname}`)
    }
  }

  public async componentDidMount() {
    const {
      links: {views: viewsLink},
    } = this.props

    try {
      await this.props.getSources()
      await this.setCurrentSource()
      await this.props.getConfig(viewsLink)
    } catch (e) {
      console.error('Failed to get sources and buckets for logs')
    }
  }

  public render() {
    const {filters, searchStatus} = this.props

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
            <LoadingStatus status={searchStatus} lower={0} upper={0} />
          </div>
        </div>
      </>
    )
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
  private handleChangeLiveUpdatingStatus = async (): Promise<void> => {
    const {liveUpdating} = this.state

    if (liveUpdating === true) {
      this.setState({liveUpdating: false})
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
      this.updateSearchStatus(SearchStatus.UpdatingFilters)
    } else {
      this.updateSearchStatus(SearchStatus.Loading)
    }
  }

  private handleFilterDelete = (id: string): void => {
    this.props.removeFilter(id)
    this.updateSearchStatus(SearchStatus.UpdatingFilters)
  }

  private handleFilterChange = async (
    id: string,
    operator: string,
    value: string
  ): Promise<void> => {
    this.props.changeFilter(id, operator, value)
    this.updateSearchStatus(SearchStatus.UpdatingFilters)
  }

  private updateSearchStatus(status: SearchStatus) {
    if (this.props.searchStatus !== SearchStatus.SourceError) {
      this.props.setSearchStatus(status)
    }
  }

  private handleClearFilters = async (): Promise<void> => {
    this.props.clearFilters()
  }

  private handleChooseSource = async (sourceID: string) => {
    const source = this.props.sources.find(s => s.id === sourceID)
    this.props.setSearchStatus(SearchStatus.Clearing)
    await this.props.getSourceAndPopulateBuckets(source.links.self)
  }

  private handleChooseBucket = async (bucket: Bucket) => {
    this.props.setSearchStatus(SearchStatus.Clearing)
    await this.props.setBucketAsync(bucket)
    this.props.setSearchStatus(SearchStatus.UpdatingBucket)
  }

  private handleUpdateTruncation = (isTruncated: boolean) => {
    const {logConfig} = this.props

    this.props.updateConfig({
      ...logConfig,
      isTruncated,
    })
  }

  private get isTruncated(): boolean {
    return this.props.logConfig.isTruncated
  }

  private get isLiveUpdating(): boolean {
    return !!this.state.liveUpdating
  }

  /**
   * Handle choosing a custom time
   * @param time the custom date selected
   */
  private handleChooseCustomTime = async (__: string) => {
    this.setState({liveUpdating: false})
    // TODO: handle updating custom time in LogState
  }

  /**
   * Handle choosing a relative time
   * @param time the epoch time selected
   */
  private handleChooseRelativeTime = async (time: number) => {
    if (time === NOW) {
      this.setState({liveUpdating: true})
    } else {
      this.setState({liveUpdating: false})
    }
    // TODO: handle updating time in LogState
  }

  /**
   * Toggle log config options overlay visibilty
   */
  private handleToggleOverlay = (): void => {}
}

const mapStateToProps = ({
  sources,
  links,
  logs: {
    currentSource,
    currentBuckets,
    currentBucket,
    filters,
    logConfig,
    searchStatus,
  },
}): StateProps => ({
  links,
  sources,
  filters,
  logConfig,
  searchStatus,
  currentSource,
  currentBucket,
  currentBuckets,
})

const mapDispatchToProps: DispatchProps = {
  notify: notifyAction,
  getSources: getSourcesAsync,
  addFilter: logActions.addFilter,
  updateConfig: logActions.setConfig,
  removeFilter: logActions.removeFilter,
  changeFilter: logActions.changeFilter,
  clearFilters: logActions.clearFilters,
  getConfig: logActions.getLogConfigAsync,
  setSearchStatus: logActions.setSearchStatus,
  setBucketAsync: logActions.setBucketAsync,
  getSourceAndPopulateBuckets: logActions.getSourceAndPopulateBucketsAsync,
}

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(LogsPage)
)
