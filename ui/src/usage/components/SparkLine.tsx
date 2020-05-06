import React, {FC} from 'react'

import {Panel, ComponentSize, InfluxColors} from '@influxdata/clockface'
import SparkLineContents from './SparkLineContents'
import {UsageTable} from 'src/types'

interface Props {
  table: UsageTable
  title: string
  column: string
  groupColumns: string[]
  isGrouped: boolean
  units: any
}

const formatYValue = (value, units) => {
  return `${value} ${units}`
}

const SparkLine: FC<Props> = ({
  table,
  title,
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
          yFormatter={value => formatYValue(value, units)}
          isGrouped={isGrouped}
        />
      </Panel.Body>
    </Panel>
  )
}

export default SparkLine
