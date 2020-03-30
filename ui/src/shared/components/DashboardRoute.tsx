import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {setDashboard} from 'src/shared/actions/currentDashboard'
import {getVariables} from 'src/variables/selectors'
import {selectValue} from 'src/variables/actions/creators'
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

// Util function that parses out a shallow object / array from
// the url search params
function parseURLVariables(searchString: string) {
  if (!searchString) {
    return {}
  }

  const urlSearch = new URLSearchParams(searchString)
  const output = {}
  let ni, breakup, varKey

  for (ni of urlSearch.entries()) {
    if (!/([^\[])+\[.*\]\s*$/.test(ni[0])) {
      output[ni[0]] = ni[1]
      continue
    }

    breakup = /(([^\[])+)\[(.*)\]\s*$/.exec(ni[0])
    varKey = breakup[1]

    if (!output.hasOwnProperty(varKey)) {
      if (!breakup[3]) {
        output[varKey] = []
      } else {
        output[varKey] = {}
      }
    }

    if (breakup[3]) {
      // had a case of empty object property being first
      if (Array.isArray(output[varKey])) {
        output[varKey] = {
          '': output[varKey],
        }
      }

      output[varKey][breakup[3]] = ni[1]
      continue
    }

    // got a blank object property
    if (!Array.isArray(output[varKey])) {
      output[varKey][''] = ni[1]
      continue
    }

    output[varKey].push(ni[1])
  }

  return output
}

// returns a list of variables that need updating
function filterVars(variables: Variable[], selections): Variable[] {
  if (!selections) {
    return []
  }

  return variables.filter(v => {
    return (
      selections.hasOwnProperty(v.name) &&
      (!v.selected || v.selected[0] !== selections[v.name])
    )
  })
}

class DashboardRoute extends PureComponent<Props> {
  check(props) {
    const {dashboard, updateDashboard, variables, selectValue} = props
    const dashboardID = props.params.dashboardID
    const urlVars = parseURLVariables(props.location.search)

    variables.forEach(v => {
      let val

      if (v.selected) {
        val = v.selected[0]
      }

      if (urlVars.vars && urlVars.vars.hasOwnProperty(v.name)) {
        val = urlVars.vars[v.name]
      }

      if (!val) {
        return
      }

      selectValue(dashboardID, v.id, val)
    })

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
  const variables = getVariables(state, state.currentDashboard.id)
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
