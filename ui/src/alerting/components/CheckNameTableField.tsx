// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

interface Props {
  row: {checkName: string; checkID: string}
}

const CheckNameTableField: FC<Props> = ({row: {checkName, checkID}}) => {
  return <Link to={`../alerting/checks/${checkID}/edit`}>{checkName}</Link>
}

export default CheckNameTableField
