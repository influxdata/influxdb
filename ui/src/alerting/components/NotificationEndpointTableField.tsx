// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

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
  const href = formatOrgRoute(
    `/alerting/endpoints/${notificationEndpointID}/edit`
  )

  return <Link to={href}>{notificationEndpointName}</Link>
}

export default NotificationEndpointTableField
