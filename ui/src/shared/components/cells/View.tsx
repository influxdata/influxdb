// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import Markdown from 'src/shared/components/views/Markdown'
import RefreshingView from 'src/shared/components/RefreshingView'

// Types
import {TimeRange} from 'src/types'
import {View, Check} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  view: View
  check: Partial<Check>
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
    const {view, timeRange, manualRefresh, check} = this.props
    const {dashboardID} = this.props.params

    if (view && view.properties) {
      if (view.properties.type === 'markdown') {
        return <Markdown text={view.properties.note} />
      }
      return (
        <RefreshingView
          timeRange={timeRange}
          check={check}
          properties={view.properties}
          manualRefresh={manualRefresh}
          dashboardID={dashboardID}
        />
      )
    }

    return null
  }
}

export default withRouter<OwnProps>(ViewComponent)
