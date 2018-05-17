import React, {SFC, ReactElement} from 'react'

import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'

import {KAPACITOR_TOOLTIP_COPY} from 'src/sources/constants'

const InfluxTableHead: SFC<{}> = (): ReactElement<
  HTMLTableHeaderCellElement
> => {
  return (
    <thead>
      <tr>
        <th className="source-table--connect-col" />
        <th>InfluxDB Connection</th>
        <th className="text-right" />
        <th>Services Connection</th>
        <th>
          Kapacitor Connection
          <QuestionMarkTooltip
            tipID="kapacitor-node-helper"
            tipContent={KAPACITOR_TOOLTIP_COPY}
          />
        </th>
      </tr>
    </thead>
  )
}

export default InfluxTableHead
