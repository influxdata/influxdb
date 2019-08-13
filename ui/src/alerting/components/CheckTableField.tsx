// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

interface Props {
  row: {check: string; checkID: string}
}

const CheckTableField: FC<Props> = ({row: {check, checkID}}) => {
  const href = formatOrgRoute(`/alerting/checks/${checkID}/edit`)

  return <Link to={href}>{check}</Link>
}

export default CheckTableField
