import React, {SFC} from 'react'

const InfluxTableHead: SFC = (): JSX.Element => {
  return (
    <thead>
      <tr>
        <th className="source-table--connect-col" />
        <th>InfluxDB Connection</th>
        <th className="text-right" />
      </tr>
    </thead>
  )
}

export default InfluxTableHead
