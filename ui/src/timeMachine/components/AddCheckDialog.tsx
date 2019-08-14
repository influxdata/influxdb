// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {Link} from 'react-router'
import {Button, ComponentColor, IconFont} from '@influxdata/clockface'

// Actions
import {convertToCheckView, setTimeMachineCheck} from 'src/timeMachine/actions'

// Constants
import {DEFAULT_THRESHOLD_CHECK} from 'src/alerting/constants'

// Types
import {AppState, RemoteDataState} from 'src/types'

interface StateProps {
  orgID: string
}

interface DispatchProps {
  onSetTimeMachineCheck: typeof setTimeMachineCheck
  onConvertToCheckView: typeof convertToCheckView
}

type Props = StateProps & DispatchProps

const AddCheckDialog: FC<Props> = ({
  orgID,
  onSetTimeMachineCheck,
  onConvertToCheckView,
}) => {
  const handleClick = () => {
    // TODO: Move the current check state into the time machine reducer, then
    // handle this state transition as part `CONVERT_TO_CHECK_VIEW` transition
    onSetTimeMachineCheck(RemoteDataState.Done, DEFAULT_THRESHOLD_CHECK)

    onConvertToCheckView()
  }

  return (
    <div className="add-alert-check-dialog">
      <p>Dashboard Cells can optionally visualize a Check.</p>
      <p>
        Checks can also be edited from the{' '}
        <Link to={`/orgs/${orgID}/alerting`}>Alerting</Link> page.
      </p>
      <Button
        text="Create New Check"
        onClick={handleClick}
        color={ComponentColor.Primary}
        icon={IconFont.Plus}
      />
    </div>
  )
}

const mstp = (state: AppState): StateProps => {
  return {orgID: state.orgs.org.id}
}

const mdtp = {
  onSetTimeMachineCheck: setTimeMachineCheck,
  onConvertToCheckView: convertToCheckView,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(AddCheckDialog)
