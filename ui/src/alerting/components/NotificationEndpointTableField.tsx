// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Reducers
import {getEndpointIDs} from 'src/alerting/reducers/notifications/endpoints'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Types
import {NotificationRow, AppState} from 'src/types'

interface OwnProps {
  row: NotificationRow
}
interface StateProps {
  endpointIDs: {[x: string]: boolean}
}

type Props = OwnProps & StateProps

const NotificationEndpointTableField: FC<Props> = ({
  row: {notificationEndpointName, notificationEndpointID},
  endpointIDs,
}) => {
  if (!endpointIDs[notificationEndpointID]) {
    return (
      <div
        className="endpoint-name-field"
        title="This endpoint no longer exists"
      >
        {notificationEndpointName}
      </div>
    )
  }

  const href = formatOrgRoute(
    `/alerting/endpoints/${notificationEndpointID}/edit`
  )

  return <Link to={href}>{notificationEndpointName}</Link>
}

const mstp = (state: AppState) => {
  return {
    endpointIDs: getEndpointIDs(state.endpoints),
  }
}

export default connect<StateProps>(mstp)(NotificationEndpointTableField)
