// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {isEmpty} from 'lodash'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

// Components
import {EmptyState, ComponentSize} from 'src/clockface'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'

// Utils
import {
  getVariablesForDashboard,
  getDashboardValuesStatus,
  getDashboardVariablesStatus,
} from 'src/variables/selectors'

// Styles
import 'src/dashboards/components/variablesControlBar/VariablesControlBar.scss'

// Actions
import {moveVariable} from 'src/variables/actions'

// Types
import {AppState} from 'src/types'
import {Variable} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {RemoteDataState} from 'src/types'
import DraggableDropdown from 'src/dashboards/components/variablesControlBar/DraggableDropdown'

interface OwnProps {
  dashboardID: string
}

interface StateProps {
  variables: Variable[]
  valuesStatus: RemoteDataState
  variablesStatus: RemoteDataState
}

interface DispatchProps {
  moveVariable: typeof moveVariable
}

interface State {
  initialLoading: RemoteDataState
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
class VariablesControlBar extends PureComponent<Props, State> {
  public state: State = {initialLoading: RemoteDataState.Loading}

  static getDerivedStateFromProps(props, state) {
    if (
      props.valuesStatus === RemoteDataState.Done &&
      props.variablesStatus === RemoteDataState.Done &&
      state.initialLoading !== RemoteDataState.Done
    ) {
      return {initialLoading: RemoteDataState.Done}
    }
  }

  render() {
    return (
      <div className="variables-control-bar">
        <SpinnerContainer
          loading={this.state.initialLoading}
          spinnerComponent={<TechnoSpinner diameterPixels={50} />}
          className="variables-spinner-container"
        >
          {this.bar}
        </SpinnerContainer>
      </div>
    )
  }

  private get emptyBar(): JSX.Element {
    return (
      <EmptyState
        size={ComponentSize.ExtraSmall}
        customClass="variables-control-bar--empty"
      >
        <EmptyState.Text text="To see variable controls here, use a variable in a cell query" />
      </EmptyState>
    )
  }

  private get barContents(): JSX.Element {
    const {dashboardID, variables, valuesStatus} = this.props
    return (
      <>
        {variables.map((v, i) => (
          <DraggableDropdown
            key={v.id}
            name={v.name}
            id={v.id}
            index={i}
            dashboardID={dashboardID}
            moveDropdown={this.handleMoveDropdown}
          />
        ))}
        {valuesStatus === RemoteDataState.Loading && (
          <TechnoSpinner diameterPixels={18} />
        )}
      </>
    )
  }

  private get bar(): JSX.Element {
    const {variables} = this.props

    if (isEmpty(variables)) {
      return this.emptyBar
    }

    return this.barContents
  }

  private handleMoveDropdown = (
    originalIndex: number,
    newIndex: number
  ): void => {
    const {dashboardID, moveVariable} = this.props
    moveVariable(originalIndex, newIndex, dashboardID)
  }
}

const mdtp = {
  moveVariable,
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const variables = getVariablesForDashboard(state, props.dashboardID)
  const valuesStatus = getDashboardValuesStatus(state, props.dashboardID)
  const variablesStatus = getDashboardVariablesStatus(state)

  return {variables, valuesStatus, variablesStatus}
}

export default DragDropContext(HTML5Backend)(
  connect<StateProps, DispatchProps, OwnProps>(
    mstp,
    mdtp
  )(VariablesControlBar)
)
