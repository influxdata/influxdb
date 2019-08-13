// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

interface Props {
  row: {notificationRule: string; notificationRuleID: string}
}

const NotificationRuleTableField: FC<Props> = ({
  row: {notificationRule, notificationRuleID},
}) => {
  const href = formatOrgRoute(`/alerting/rules/${notificationRuleID}/edit`)

  return <Link to={href}>{notificationRule}</Link>
}

export default NotificationRuleTableField
