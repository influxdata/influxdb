import React, {Component} from 'react'
import {
  ComponentSize,
  FlexBox,
  AlignItems,
  FlexDirection,
  Panel,
} from '@influxdata/clockface'
import {fromFlux} from '@influxdata/vis'
import PanelSection from 'src/usage/components/PanelSection'
import AlertBanner from 'src/usage/components/AlertBanner'
import UsageDropdown from 'src/usage/components/UsageDropdown'
import PanelSectionBody from 'src/usage/components/PanelSectionBody'
import BillingStatsPanel from 'src/usage/components/BillingStatsPanel'
import TimeRangeDropdown from './TimeRangeDropdown'

// Types
import {
  UsageQueryStatus,
  UsageLimitStatus,
  UsageHistory,
  UsageBillingStart,
  UsageTable,
} from 'src/types'

// Constants
import {PANEL_CONTENTS_WIDTHS, RANGES} from 'src/usage/components/constants'
import {GRAPH_INFO} from 'src/usage/components/constants'

interface Props {
  history: UsageHistory
  limitStatuses: UsageLimitStatus
  billingStart: UsageBillingStart
  accountType: string
  selectedRange: string
}

interface State {
  selectedUsageID:
    | 'Writes (MB)'
    | 'Total Query Duration (s)'
    | 'Storage (GB-hr)'
}

class UsageToday extends Component<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      selectedUsageID: 'Writes (MB)',
    }
  }

  render() {
    const {
      history,
      limitStatuses,
      accountType,
      selectedRange,
      billingStart,
    } = this.props

    const {table: billingTable, status: billingStatus} = this.csvToTable(
      history.billingStats
    )

    const {table: limitsTable, status: limitsStatus} = this.csvToTable(
      history.rateLimits
    )

    const {selectedUsageID} = this.state

    return (
      <FlexBox
        alignItems={AlignItems.Stretch}
        direction={FlexDirection.Column}
        margin={ComponentSize.Small}
      >
        <AlertBanner
          limitStatuses={limitStatuses}
          isOperator={false}
          accountType={accountType}
        />
        <BillingStatsPanel
          table={billingTable}
          status={billingStatus}
          billingStart={billingStart}
          widths={PANEL_CONTENTS_WIDTHS.billingStats}
        />
        <TimeRangeDropdown
          selectedTimeRange={RANGES[selectedRange]}
          dropdownOptions={RANGES}
          onSelect={this.handleTimeRangeChange}
        />
        <Panel className="usage--panel">
          <Panel.Header>
            <h4>{`Usage ${RANGES[selectedRange]}`}</h4>
            <UsageDropdown
              selectedUsage={selectedUsageID}
              onSelect={this.handleUsageChange}
            />
          </Panel.Header>
          <PanelSection>{this.getUsageSparkline()}</PanelSection>
        </Panel>
        <Panel className="usage--panel">
          <Panel.Header>
            <h5>{`Rate Limits ${RANGES[selectedRange]}`}</h5>
          </Panel.Header>
          <PanelSection>
            {GRAPH_INFO.rateLimits.map(graphInfo => {
              return (
                <PanelSectionBody
                  table={limitsTable}
                  status={limitsStatus}
                  graphInfo={graphInfo}
                  widths={PANEL_CONTENTS_WIDTHS.rateLimits}
                  key={graphInfo.title}
                />
              )
            })}
          </PanelSection>
        </Panel>
      </FlexBox>
    )
  }

  getUsageSparkline() {
    const {history} = this.props

    const {selectedUsageID} = this.state
    switch (selectedUsageID) {
      case 'Writes (MB)':
        const {table: writeTable, status: writeStatus} = this.csvToTable(
          history.writeMB
        )
        return GRAPH_INFO.writeMB.map(graphInfo => {
          return (
            <PanelSectionBody
              table={writeTable}
              status={writeStatus}
              graphInfo={graphInfo}
              widths={PANEL_CONTENTS_WIDTHS.usage}
              key={graphInfo.title}
            />
          )
        })
      case 'Total Query Duration (s)':
        const {
          table: executionTable,
          status: executionStatus,
        } = this.csvToTable(history.executionSec)
        return GRAPH_INFO.executionSec.map(graphInfo => {
          return (
            <PanelSectionBody
              table={executionTable}
              status={executionStatus}
              graphInfo={graphInfo}
              widths={PANEL_CONTENTS_WIDTHS.usage}
              key={graphInfo.title}
            />
          )
        })
      case 'Storage (GB-hr)':
        const {table: storageTable, status: storageStatus} = this.csvToTable(
          history.storageGB
        )
        return GRAPH_INFO.storageGB.map(graphInfo => {
          return (
            <PanelSectionBody
              table={storageTable}
              status={storageStatus}
              graphInfo={graphInfo}
              widths={PANEL_CONTENTS_WIDTHS.usage}
              key={graphInfo.title}
            />
          )
        })
      default:
        break
    }
  }

  handleTimeRangeChange = range => {
    console.log('range: ', range)
  }

  handleUsageChange = v => {
    this.setState({selectedUsageID: v})
  }

  csvToTable = (
    csv: string
  ): {
    table: UsageTable
    status: UsageQueryStatus
  } => {
    const {table} = fromFlux(csv)

    if (!table.length) {
      return {
        status: 'empty',
        table: {columns: {}, length: 0},
      }
    }

    return {status: 'success', table}
  }
}

export default UsageToday
