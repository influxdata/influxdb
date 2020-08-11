// Libraries
import React, {PureComponent} from 'react'
import qs from 'qs'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Actions
import {setDashboard} from 'src/shared/actions/currentDashboard'
import {selectValue} from 'src/variables/actions/thunks'
import {dashboardVisit as dashboardVisitAction} from 'src/perf/actions'

// Utils
import {event} from 'src/cloud/utils/reporting'

// Selector
import {getVariables} from 'src/variables/selectors'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
interface OwnProps {
  children: React.ReactNode
}

type Props = ReduxProps &
  OwnProps &
  RouteComponentProps<{orgID: string; dashboardID: string}>

class DashboardRoute extends PureComponent<Props> {
  pendingVars: [{[key: string]: any}]

  // this function takes the hydrated variables from state
  // and runs the `selectValue` action against them if the
  // selected value in the search params doesn't match the
  // selected value in the redux store
  // urlVars represents the `vars` object variable in the
  // query params here, and unwrapping / validation is
  // handled elsewhere
  syncVariables(props, urlVars) {
    const {variables, selectValue} = props

    variables.forEach(v => {
      let val

      if (v.selected) {
        val = v.selected[0]
      }

      if (urlVars[v.name] && val !== urlVars[v.name]) {
        val = urlVars[v.name]
        selectValue(v.id, val)
      }
    })
  }

  componentDidMount() {
    const {dashboard, updateDashboard, variables, dashboardVisit} = this.props
    const dashboardID = this.props.match.params.dashboardID
    dashboardVisit(dashboardID, new Date().getTime())
    event('Dashboard Visit', {dashboardID})
    const urlVars = qs.parse(this.props.location.search, {
      ignoreQueryPrefix: true,
    })

    // always keep the dashboard in sync
    if (dashboard !== dashboardID) {
      updateDashboard(dashboardID)
    }

    // nothing to sync as the query params aren't defining
    // any variables
    if (!urlVars.hasOwnProperty('vars')) {
      return
    }

    // resource is still loading
    // we have to wait for it so that we can filter out arbitrary user input
    // from the redux state before commiting it back to localstorage
    if (!variables.length) {
      this.pendingVars = urlVars.vars
      return
    }

    this.syncVariables(this.props, urlVars.vars)
  }

  componentDidUpdate(props) {
    if (props.variables === this.props.variables) {
      return
    }

    if (!this.props.variables.length) {
      return
    }

    if (!this.pendingVars) {
      return
    }

    this.syncVariables(this.props, this.pendingVars)

    delete this.pendingVars
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

const mstp = (state: AppState) => {
  const variables = getVariables(state)

  return {
    variables,
    dashboard: state.currentDashboard.id,
  }
}

const mdtp = {
  dashboardVisit: dashboardVisitAction,
  updateDashboard: setDashboard,
  selectValue: selectValue,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(DashboardRoute))
