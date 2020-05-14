import React, {FC} from 'react'

import {
  Panel,
  ReflessPopover,
  PopoverInteraction,
  PopoverPosition,
  Appearance,
} from '@influxdata/clockface'

interface Props {
  startDate: string
}

const BillingStatsHeader: FC<Props> = ({startDate}) => {
  const today = new Date().toISOString()
  const dateRange = `${startDate} UTC to ${today} UTC`

  return (
    <Panel.Header className="usage--billing-header">
      <ReflessPopover
        distanceFromTrigger={16}
        contents={() => <>{dateRange}</>}
        appearance={Appearance.Outline}
        position={PopoverPosition.ToTheRight}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
      >
        <h4 className="usage--billing-date-range">{`Billing Stats For ${startDate} to Today`}</h4>
      </ReflessPopover>
    </Panel.Header>
  )
}

export default BillingStatsHeader
