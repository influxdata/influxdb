// Libraries
import React, {Component} from 'react'

// Components
import Markdown from 'src/shared/components/views/Markdown'
import RefreshingView from 'src/shared/components/RefreshingView'

// Types
import {TimeRange} from 'src/types'
import {View, ViewType, ViewShape} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  view: View
  timeRange: TimeRange
  autoRefresh: number
  manualRefresh: number
  onZoom: (range: TimeRange) => void
  onEditCell: () => void
}

@ErrorHandling
class ViewComponent extends Component<Props> {
  public state = {
    cellData: [],
  }

  public render() {
    const {view, onZoom, timeRange, manualRefresh} = this.props

    switch (view.properties.type) {
      case ViewShape.Empty:
      case ViewType.LogViewer:
        return this.emptyGraph
      case ViewType.Markdown:
        return <Markdown text={view.properties.note} />
      default:
        return (
          <RefreshingView
            viewID={view.id}
            onZoom={onZoom}
            timeRange={timeRange}
            properties={view.properties}
            manualRefresh={manualRefresh}
          />
        )
    }
  }

  private get emptyGraph(): JSX.Element {
    return (
      <div className="graph-empty">
        <button
          className="no-query--button btn btn-md btn-primary"
          onClick={this.props.onEditCell}
        >
          <span className="icon plus" /> Add Data
        </button>
      </div>
    )
  }
}

export default ViewComponent
