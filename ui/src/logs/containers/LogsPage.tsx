import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {
  getSourceAndPopulateNamespacesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeHistogramQueryAsync,
  changeZoomAsync,
} from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import LogViewerHeader from 'src/logs/components/LogViewerHeader'
import Graph from 'src/logs/components/LogsGraph'
import Table from 'src/logs/components/LogsTable'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/LogsFilterBar'
import LogViewerChart from 'src/logs/components/LogViewerChart'

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
  executeHistogramQueryAsync: () => void
  timeRange: TimeRange
  histogramData: object[]
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
  }

  public render() {
    const {searchString, filters} = this.state

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
            numResults={300}
            filters={filters}
            onUpdateFilters={this.handleUpdateFilters}
          />
          <Table thing="snooo" />
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
    this.props.executeHistogramQueryAsync()
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
  },
}) => ({
  sources,
  currentSource,
  currentNamespaces,
  timeRange,
  currentNamespace,
  histogramData,
})

const mapDispatchToProps = {
  getSource: getSourceAndPopulateNamespacesAsync,
  getSources: getSourcesAsync,
  setTimeRangeAsync,
  setNamespaceAsync,
  executeHistogramQueryAsync,
  changeZoomAsync,
}

export default connect(mapStateToProps, mapDispatchToProps)(LogsPage)
