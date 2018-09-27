// Libraries
import React, {Component} from 'react'

// Components
import Markdown from 'src/shared/components/views/Markdown'
import RefreshingView from 'src/shared/components/RefreshingView'

// Constants
import {text} from 'src/shared/components/views/gettingsStarted'

// Types
import {TimeRange, Template} from 'src/types'
import {View, ViewType, ViewShape} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  view: View
  timeRange: TimeRange
  templates: Template[]
  autoRefresh: number
  manualRefresh: number
  onZoom: (range: TimeRange) => void
  onSummonOverlay: () => void
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

    if (view.properties.shape === ViewShape.Empty) {
      return this.emptyGraph
    }

    if (view.properties.type === ViewType.Markdown) {
      return <Markdown text={text} />
    }

    return (
      <RefreshingView
        viewID={view.id}
        onZoom={onZoom}
        timeRange={timeRange}
        templates={templates}
        autoRefresh={autoRefresh}
        properties={view.properties}
        manualRefresh={manualRefresh}
        grabDataForDownload={this.grabDataForDownload}
      />
    )
  }

  private get emptyGraph(): JSX.Element {
    return (
      <div className="graph-empty">
        <button
          className="no-query--button btn btn-md btn-primary"
          onClick={this.props.onSummonOverlay}
        >
          <span className="icon plus" /> Add Data
        </button>
      </div>
    )
  }

  private grabDataForDownload = cellData => {
    this.setState({cellData})
  }
}

export default ViewComponent
