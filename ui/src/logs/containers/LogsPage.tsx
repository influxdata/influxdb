import React, {PureComponent} from 'react'
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
import LogViewerChart from 'src/logs/components/LogViewerChart'
import GraphContainer from 'src/logs/components/LogsGraphContainer'
import TableContainer from 'src/logs/components/LogsTableContainer'

import {Source, Namespace, TimeRange} from 'src/types'

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

class LogsPage extends PureComponent<Props> {
  public componentDidUpdate() {
    if (!this.props.currentSource) {
      this.props.getSource(this.props.sources[0].id)
    }
  }

  public componentDidMount() {
    this.props.getSources()
  }

  public render() {
    return (
      <div className="page">
        <div className="page-header full-width">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Log Viewer</h1>
            </div>
            <div className="page-header__right">{this.header}</div>
          </div>
        </div>
        <div className="page-contents logs-viewer">
          <GraphContainer>{this.chart}</GraphContainer>
          <TableContainer thing="snooo" />
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
