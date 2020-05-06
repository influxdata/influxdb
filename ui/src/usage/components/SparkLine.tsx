import React, {FC} from 'react'

import {Panel, ComponentSize, InfluxColors} from '@influxdata/clockface'
import SparkLineContents from './SparkLineContents'
import {UsageTable, UsageQueryStatus} from 'src/types'

interface Props {
  table: UsageTable
  title: string
  column: string
  groupColumns: string[]
  isGrouped: boolean
  units: any
  status: UsageQueryStatus
}

const formatYValue = (value, units) => {
  return `${value} ${units}`
}

const SparkLine: FC<Props> = ({
  table,
  title,
  status,
  column,
  groupColumns,
  isGrouped,
  units,
}) => {
  return (
    <Panel backgroundColor={InfluxColors.Onyx}>
      <Panel.Header size={ComponentSize.ExtraSmall}>
        <h5>{title}</h5>
      </Panel.Header>
      <Panel.Body size={ComponentSize.ExtraSmall}>
        <SparkLineContents
          table={table}
          column={column}
          groupColumns={groupColumns}
          yFormatter={formatYValue(value, units)}
          status={status}
          isGrouped={isGrouped}
        />
      </Panel.Body>
    </Panel>
  )
}

export default SparkLine
