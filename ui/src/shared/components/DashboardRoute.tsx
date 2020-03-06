import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {setDashboard} from 'src/shared/actions/currentDashboard'
import {AppState} from 'src/types'

interface StateProps {
  dashboard: string
}

interface DispatchProps {
  updateDashboard: typeof setDashboard
}

type Props = StateProps & DispatchProps & WithRouterProps

class DashboardRoute extends PureComponent<Props> {
  check(props) {
    const {dashboard, updateDashboard} = props
    const dashboardID = props.params.dashboardID

    if (dashboard !== dashboardID) {
      updateDashboard(dashboardID)
    }
  }

  componentDidMount() {
    this.check(this.props)
  }

  componentWillUnmount() {
    this.props.updateDashboard(null)
  }

  render() {
    if (!this.props.dashboard) {
      return false
    }

    return <>{this.props.children}</>
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    dashboard: state.currentDashboard.id,
  }
}

const mdtp: DispatchProps = {
  updateDashboard: setDashboard,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<{}>(DashboardRoute))
