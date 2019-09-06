// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Selectors
import {getCheckIDs} from 'src/alerting/reducers/checks'

// Types
import {StatusRow, NotificationRow, AppState} from 'src/types'

interface OwnProps {
  row: StatusRow | NotificationRow
}

interface StateProps {
  checkIDs: {[x: string]: boolean}
}

type Props = StateProps & OwnProps

const CheckTableField: FC<Props> = ({row: {checkName, checkID}, checkIDs}) => {
  if (!checkIDs[checkID]) {
    return (
      <div
        className="check-name-field"
        title="The check that created this no longer exists"
      >
        {checkName}
      </div>
    )
  }

  const href = formatOrgRoute(`/alerting/checks/${checkID}/edit`)

  return <Link to={href}>{checkName}</Link>
}

const mstp = (state: AppState) => {
  return {
    checkIDs: getCheckIDs(state.checks),
  }
}

export default connect<StateProps>(mstp)(CheckTableField)
