// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'
import {getResourceIDs} from 'src/alerting/selectors'

// Types
import {NotificationRow, AppState, ResourceType} from 'src/types'

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
    endpointIDs: getResourceIDs(state, ResourceType.NotificationEndpoints),
  }
}

export default connect<StateProps>(mstp)(NotificationEndpointTableField)
