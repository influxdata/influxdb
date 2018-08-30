// Libraries
import React, {Component} from 'react'

// Components
import RefreshingGraph from 'src/shared/components/RefreshingGraph'

// Types
import {TimeRange, Template} from 'src/types'
import {View} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  view: View
  timeRange: TimeRange
  templates: Template[]
  autoRefresh: number
  manualRefresh: number
  onZoom: (range: TimeRange) => void
}

@ErrorHandling
class ViewComponent extends Component<Props> {
  public state = {
    cellData: [],
  }

  public render() {
    const {
      view,
      onZoom,
      timeRange,
      autoRefresh,
      manualRefresh,
      templates,
    } = this.props

    return (
      <RefreshingGraph
        viewID={view.id}
        onZoom={onZoom}
        timeRange={timeRange}
        templates={templates}
        autoRefresh={autoRefresh}
        options={view.properties}
        manualRefresh={manualRefresh}
        grabDataForDownload={this.grabDataForDownload}
      />
    )
  }

  private grabDataForDownload = cellData => {
    this.setState({cellData})
  }
}

export default ViewComponent
