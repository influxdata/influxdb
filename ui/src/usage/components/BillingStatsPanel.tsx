// Libraries
import React from 'react'
import {getOrgsBilling} from './mocks'

// Hooks
import useClient from './useClient'

import {Panel} from '@influxdata/clockface'

import PanelSection from './PanelSection'
import PanelSectionBody from './PanelSectionBody'

import {GRAPH_INFO} from './constants'
import BillingStatsHeader from './BillingStatsHeader'

import {RemoteDataState} from 'src/types'

const billingStats = () => {
  const titles = GRAPH_INFO.titles
  return GRAPH_INFO.billingStats.filter(stat => titles.includes(stat.title))
}

const {Done} = RemoteDataState

const BillingStatsPanel = ({table, widths}) => {
  const [status, data, error] = useClient(() => getOrgsBilling)
  console.log(status, data, error)

  if (status !== Done || data === null) {
    return <div>'Loading...'</div>
  }

  const {startDate} = data

  return (
    <Panel className="usage--panel billing-stats--panel">
      <BillingStatsHeader startDate={startDate} />
      <PanelSection>
        {billingStats().map(graphInfo => {
          return (
            <PanelSectionBody
              table={table}
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
