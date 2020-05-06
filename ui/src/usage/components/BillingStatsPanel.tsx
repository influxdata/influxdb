import React from 'react'

import {
  Panel,
  ReflessPopover,
  PopoverInteraction,
  PopoverPosition,
} from '@influxdata/clockface'

import PanelSection from './PanelSection'
import PanelSectionBody from './PanelSectionBody'

import {GRAPH_INFO} from './constants'

const billingStats = () => {
  const titles = GRAPH_INFO.titles
  return GRAPH_INFO.billingStats.filter(stat => titles.includes(stat.title))
}

const BillingStatsPanel = ({
  table,
  status,
  widths,
  billingStart: {date: billingStartDate, time: billingStartTime},
}) => {
  const today = new Date().toISOString()
  const dateRange = `${billingStartTime} UTC to ${today} UTC`

  return (
    <Panel className="usage--panel billing-stats--panel">
      <Panel.Header className="usage--billing-header">
        <ReflessPopover
          distanceFromTrigger={16}
          contents={() => <>{dateRange}</>}
          position={PopoverPosition.ToTheRight}
          showEvent={PopoverInteraction.Hover}
          hideEvent={PopoverInteraction.Hover}
        >
          <h4 className="usage--billing-date-range">{`Billing Stats For ${billingStartDate} to Today`}</h4>
        </ReflessPopover>
      </Panel.Header>
      <PanelSection>
        {billingStats().map(graphInfo => {
          return (
            <PanelSectionBody
              table={table}
              status={status}
              graphInfo={graphInfo}
              widths={widths}
              key={graphInfo.title}
            />
          )
        })}
      </PanelSection>
    </Panel>
  )
}

export default BillingStatsPanel
