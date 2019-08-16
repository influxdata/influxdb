// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {Link} from 'react-router'
import {Button, ComponentColor, IconFont} from '@influxdata/clockface'

// Actions
import {convertToCheckView} from 'src/timeMachine/actions'

// Types
import {AppState} from 'src/types'

interface StateProps {
  orgID: string
}

interface DispatchProps {
  onConvertToCheckView: typeof convertToCheckView
}

type Props = StateProps & DispatchProps

const AddCheckDialog: FC<Props> = ({orgID, onConvertToCheckView}) => {
  const handleClick = () => {
    onConvertToCheckView()
  }

  return (
    <div className="add-alert-check-dialog">
      <p>
        Dashboard Cells can optionally visualize a <strong>Check</strong>
      </p>
      <p>
        Checks can also be edited from the{' '}
        <Link to={`/orgs/${orgID}/alerting`}>Monitoring & Alerting</Link> page
      </p>
      <Button
        text="Add a Check"
        onClick={handleClick}
        color={ComponentColor.Primary}
        icon={IconFont.Plus}
        titleText="Add a Check to monitor this data"
      />
    </div>
  )
}

const mstp = (state: AppState): StateProps => {
  return {orgID: state.orgs.org.id}
}

const mdtp = {
  onConvertToCheckView: convertToCheckView,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(AddCheckDialog)
