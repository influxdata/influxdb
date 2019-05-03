// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import Markdown from 'src/shared/components/views/Markdown'
import RefreshingView from 'src/shared/components/RefreshingView'

// Types
import {TimeRange} from 'src/types'
import {ViewType, ViewShape, View} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  view: View
  timeRange: TimeRange
  manualRefresh: number
  onEditCell: () => void
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class ViewComponent extends Component<Props> {
  public state = {
    cellData: [],
  }

  public render() {
    const {view, timeRange, manualRefresh} = this.props
    const {dashboardID} = this.props.params

    switch (view.properties.type) {
      case ViewShape.Empty:
        return this.emptyGraph
      case ViewType.Markdown:
        return <Markdown text={view.properties.note} />
      default:
        return (
          <RefreshingView
            timeRange={timeRange}
            properties={view.properties}
            manualRefresh={manualRefresh}
            inView={true}
            dashboardID={dashboardID}
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

export default withRouter<OwnProps>(ViewComponent)
