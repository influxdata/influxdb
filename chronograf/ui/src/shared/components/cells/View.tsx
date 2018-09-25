// Libraries
import React, {Component} from 'react'

// Components
import RefreshingGraph from 'src/shared/components/RefreshingGraph'
import Markdown from 'src/shared/components/views/Markdown'

// Types
import {TimeRange, Template} from 'src/types'
import {View, ViewType} from 'src/types/v2'
import {text} from 'src/shared/components/views/gettingsStarted'

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

    if (view.properties.type === ViewType.Markdown) {
      return <Markdown text={text} />
    }

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
