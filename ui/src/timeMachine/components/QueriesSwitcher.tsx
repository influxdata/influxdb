// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Button,
  ConfirmationButton,
  Appearance,
  ComponentColor,
} from '@influxdata/clockface'

// Actions
import {
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
  resetActiveQuerySwitchToBuilder,
} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'
import {
  confirmationState,
  ConfirmationState,
} from 'src/timeMachine/utils/queryBuilder'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class TimeMachineQueriesSwitcher extends PureComponent<Props> {
  public render() {
    const {
      onEditAsFlux,
      onEditWithBuilder,
      onResetAndEditWithBuilder,
    } = this.props
    const {editMode, text, builderConfig} = this.props.activeQuery
    const scriptMode = editMode !== 'builder'

    let button = (
      <Button
        text="Script Editor"
        titleText="Switch to Script Editor"
        onClick={onEditAsFlux}
        testID="switch-to-script-editor"
      />
    )

    if (scriptMode) {
      button = (
        <Button
          text="Query Builder"
          titleText="Switch to Query Builder"
          onClick={onEditWithBuilder}
          testID="switch-to-query-builder"
        />
      )
    }

    if (
      scriptMode &&
      confirmationState(text, builderConfig) === ConfirmationState.Required
    ) {
      button = (
        <ConfirmationButton
          popoverColor={ComponentColor.Danger}
          popoverAppearance={Appearance.Outline}
          popoverStyle={{width: '400px'}}
          confirmationLabel="Switching to Query Builder mode will discard any changes you
                have made using Flux. This cannot be recovered."
          confirmationButtonText="Switch to Builder"
          text="Query Builder"
          onConfirm={onResetAndEditWithBuilder}
          testID="switch-query-builder-confirm"
        />
      )
    }

    return button
  }
}

const mstp = (state: AppState) => {
  const activeQuery = getActiveQuery(state)

  return {activeQuery}
}

const mdtp = {
  onEditWithBuilder: editActiveQueryWithBuilder,
  onResetAndEditWithBuilder: resetActiveQuerySwitchToBuilder,
  onEditAsFlux: editActiveQueryAsFlux,
}

const connector = connect(mstp, mdtp)

export default connector(TimeMachineQueriesSwitcher)
