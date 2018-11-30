// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, Dropdown} from 'src/clockface'

// Actions
import {
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
  editActiveQueryAsInfluxQL,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'
import {CONFIRM_LEAVE_ADVANCED_MODE} from 'src/shared/copy/v2'

// Types
import {AppState} from 'src/types/v2'
import {TimeMachineEditor} from 'src/types/v2/timeMachine'

interface StateProps {
  activeQueryEditor: TimeMachineEditor
}

interface DispatchProps {
  onEditWithBuilder: typeof editActiveQueryWithBuilder
  onEditAsFlux: typeof editActiveQueryAsFlux
  onEditAsInfluxQL: typeof editActiveQueryAsInfluxQL
}

type Props = StateProps & DispatchProps

class TimeMachineQueriesSwitcher extends PureComponent<Props> {
  public render() {
    const {activeQueryEditor, onEditAsFlux, onEditAsInfluxQL} = this.props

    if (activeQueryEditor !== TimeMachineEditor.QueryBuilder) {
      return (
        <Button
          text="Visual Query Builder"
          onClick={this.handleEditWithBuilder}
        />
      )
    }

    return (
      <Dropdown
        selectedID=""
        titleText="Edit Query As..."
        widthPixels={130}
        onChange={this.handleChooseLanguage}
      >
        <Dropdown.Item id={TimeMachineEditor.FluxEditor} value={onEditAsFlux}>
          Flux
        </Dropdown.Item>
        <Dropdown.Item
          id={TimeMachineEditor.InfluxQLEditor}
          value={onEditAsInfluxQL}
        >
          InfluxQL
        </Dropdown.Item>
      </Dropdown>
    )
  }

  private handleEditWithBuilder = (): void => {
    const {onEditWithBuilder} = this.props

    if (window.confirm(CONFIRM_LEAVE_ADVANCED_MODE)) {
      onEditWithBuilder()
    }
  }

  private handleChooseLanguage = (actionCreator): void => {
    actionCreator()
  }
}

const mstp = (state: AppState) => {
  const {activeQueryEditor} = getActiveTimeMachine(state)

  return {activeQueryEditor}
}

const mdtp = {
  onEditWithBuilder: editActiveQueryWithBuilder,
  onEditAsFlux: editActiveQueryAsFlux,
  onEditAsInfluxQL: editActiveQueryAsInfluxQL,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueriesSwitcher)
