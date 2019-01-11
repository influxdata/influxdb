// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from 'src/clockface'

// Actions
import {
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
  editActiveQueryAsInfluxQL,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {
  getActiveQuery,
  getActiveQuerySource,
} from 'src/shared/selectors/timeMachines'
import {CONFIRM_LEAVE_ADVANCED_MODE} from 'src/shared/copy/v2'

// Types
import {AppState, QueryEditMode, Source} from 'src/types/v2'

interface StateProps {
  editMode: QueryEditMode
  sourceType: Source.TypeEnum
}

interface DispatchProps {
  onEditWithBuilder: typeof editActiveQueryWithBuilder
  onEditAsFlux: typeof editActiveQueryAsFlux
  onEditAsInfluxQL: typeof editActiveQueryAsInfluxQL
}

type Props = StateProps & DispatchProps

class TimeMachineQueriesSwitcher extends PureComponent<Props> {
  public render() {
    const {editMode, sourceType, onEditAsFlux, onEditAsInfluxQL} = this.props

    if (editMode !== QueryEditMode.Builder) {
      return (
        <Button
          text="Switch to Query Builder"
          onClick={this.handleEditWithBuilder}
        />
      )
    }

    if (sourceType === Source.TypeEnum.V1) {
      return (
        <Button text="Switch to Script Editor" onClick={onEditAsInfluxQL} />
      )
    }

    return <Button text="Switch to Script Editor" onClick={onEditAsFlux} />
  }

  private handleEditWithBuilder = (): void => {
    const {onEditWithBuilder} = this.props

    if (window.confirm(CONFIRM_LEAVE_ADVANCED_MODE)) {
      onEditWithBuilder()
    }
  }
}

const mstp = (state: AppState) => {
  const editMode = getActiveQuery(state).editMode
  const sourceType = getActiveQuerySource(state).type

  return {editMode, sourceType}
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
