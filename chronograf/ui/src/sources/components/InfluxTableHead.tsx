import React, {SFC, ReactElement} from 'react'

const InfluxTableHead: SFC = (): ReactElement<HTMLTableHeaderCellElement> => {
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
