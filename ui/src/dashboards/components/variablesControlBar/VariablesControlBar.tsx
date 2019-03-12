// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import VariableDropdown from 'src/dashboards/components/variablesControlBar/VariableDropdown'
import {EmptyState, ComponentSize} from 'src/clockface'

// Utils
import {getVariablesForDashboard} from 'src/variables/selectors'

// Styles
import 'src/dashboards/components/variablesControlBar/VariablesControlBar.scss'

// Actions
import {selectValue} from 'src/variables/actions'

// Types
import {AppState} from 'src/types/v2'
import {Variable} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  dashboardID: string
}

interface StateProps {
  variables: Variable[]
}

interface DispatchProps {
  selectValue: typeof selectValue
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
class VariablesControlBar extends PureComponent<Props> {
  render() {
    const {dashboardID, variables} = this.props

    if (_.isEmpty(variables)) {
      return (
        <div className="variables-control-bar">
          <EmptyState
            size={ComponentSize.ExtraSmall}
            customClass="variables-control-bar--empty"
          >
            <EmptyState.Text text="To see variable controls here, use a variable in a cell query" />
          </EmptyState>
        </div>
      )
    }

    return (
      <div className="variables-control-bar">
        {variables.map(v => {
          return (
            <VariableDropdown
              key={v.id}
              name={v.name}
              variableID={v.id}
              dashboardID={dashboardID}
              onSelect={this.handleSelectValue}
            />
          )
        })}
      </div>
    )
  }

  private handleSelectValue = (variableID: string, value: string) => {
    const {selectValue, dashboardID} = this.props
    selectValue(dashboardID, variableID, value)
  }
}

const mdtp = {
  selectValue: selectValue,
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {variables: getVariablesForDashboard(state, props.dashboardID)}
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariablesControlBar)
