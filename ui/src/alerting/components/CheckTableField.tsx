// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Types
import {StatusRow, NotificationRow} from 'src/types'

interface Props {
  row: StatusRow | NotificationRow
}

const CheckTableField: FC<Props> = ({row: {checkName, checkID}}) => {
  const href = formatOrgRoute(`/alerting/checks/${checkID}/edit`)

  return <Link to={href}>{checkName}</Link>
}

export default CheckTableField
