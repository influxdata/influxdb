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
import {getActiveQuery} from 'src/shared/selectors/timeMachines'
import {CONFIRM_LEAVE_ADVANCED_MODE} from 'src/shared/copy/v2'

// Types
import {AppState, QueryEditMode} from 'src/types/v2'

interface StateProps {
  editMode: QueryEditMode
}

interface DispatchProps {
  onEditWithBuilder: typeof editActiveQueryWithBuilder
  onEditAsFlux: typeof editActiveQueryAsFlux
  onEditAsInfluxQL: typeof editActiveQueryAsInfluxQL
}

type Props = StateProps & DispatchProps

class TimeMachineQueriesSwitcher extends PureComponent<Props> {
  public render() {
    const {editMode, onEditAsFlux, onEditAsInfluxQL} = this.props

    if (editMode !== QueryEditMode.Builder) {
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
        <Dropdown.Item id={'influxQL'} value={onEditAsFlux}>
          Flux
        </Dropdown.Item>
        <Dropdown.Item id={'flux'} value={onEditAsInfluxQL}>
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
  const editMode = getActiveQuery(state).editMode

  return {editMode}
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
