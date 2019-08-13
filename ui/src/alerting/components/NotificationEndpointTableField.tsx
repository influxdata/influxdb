// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

interface Props {
  row: {notificationEndpoint: string; notificationEndpointID: string}
}

const NotificationEndpointTableField: FC<Props> = ({
  row: {notificationEndpoint, notificationEndpointID},
}) => {
  const href = formatOrgRoute(
    `/alerting/endpoints/${notificationEndpointID}/edit`
  )

  return <Link to={href}>{notificationEndpoint}</Link>
}

export default NotificationEndpointTableField
