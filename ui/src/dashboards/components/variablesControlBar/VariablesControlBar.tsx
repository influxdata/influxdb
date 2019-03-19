// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import VariableDropdown from 'src/dashboards/components/variablesControlBar/VariableDropdown'
import {EmptyState, ComponentSize} from 'src/clockface'
import {TechnoSpinner} from '@influxdata/clockface'

// Utils
import {
  getVariablesForDashboard,
  getDashboardValuesStatus,
} from 'src/variables/selectors'

// Styles
import 'src/dashboards/components/variablesControlBar/VariablesControlBar.scss'

// Types
import {AppState} from 'src/types/v2'
import {Variable} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  dashboardID: string
}

interface StateProps {
  variables: Variable[]
  valuesStatus: RemoteDataState
}

type Props = StateProps & OwnProps

@ErrorHandling
class VariablesControlBar extends PureComponent<Props> {
  render() {
    const {dashboardID, variables, valuesStatus} = this.props

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
        {variables.map(v => (
          <VariableDropdown
            key={v.id}
            name={v.name}
            variableID={v.id}
            dashboardID={dashboardID}
          />
        ))}
        {valuesStatus === RemoteDataState.Loading && (
          <TechnoSpinner diameterPixels={18} />
        )}
      </div>
    )
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const variables = getVariablesForDashboard(state, props.dashboardID)
  const valuesStatus = getDashboardValuesStatus(state, props.dashboardID)

  return {variables, valuesStatus}
}

export default connect<StateProps, {}, OwnProps>(mstp)(VariablesControlBar)
