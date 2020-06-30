import React, {PureComponent} from 'react'
import qs from 'qs'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {setDashboard} from 'src/shared/actions/currentDashboard'
import {getVariables} from 'src/variables/selectors'
import {selectValue} from 'src/variables/actions/thunks'
import {AppState, Variable} from 'src/types'

interface StateProps {
  variables: Variable[]
  dashboard: string
}

interface DispatchProps {
  updateDashboard: typeof setDashboard
  selectValue: typeof selectValue
}

type Props = StateProps & DispatchProps & WithRouterProps

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
    const {dashboard, updateDashboard, variables} = this.props
    const dashboardID = this.props.params.dashboardID
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

const mstp = (state: AppState): StateProps => {
  const variables = getVariables(state)

  return {
    variables,
    dashboard: state.currentDashboard.id,
  }
}

const mdtp: DispatchProps = {
  updateDashboard: setDashboard,
  selectValue: selectValue,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<{}>(DashboardRoute))
