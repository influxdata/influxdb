// Libraries
import React, {FC, useContext} from 'react'
import {Link} from 'react-router-dom'

// Context
import {ResourceIDsContext} from 'src/alerting/components/AlertHistoryIndex'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Types
import {NotificationRow} from 'src/types'

interface Props {
  row: NotificationRow
}

const NotificationEndpointTableField: FC<Props> = ({
  row: {notificationEndpointName, notificationEndpointID},
}) => {
  const {endpointIDs} = useContext(ResourceIDsContext)

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

export default NotificationEndpointTableField
