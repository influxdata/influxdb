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
import {GRAPH_INFO} from 'src/usage/components/Constants'
import AlertBanner from 'src/usage/components/AlertBanner'
import UsageDropdown from 'src/usage/components/UsageDropdown'
import {
  QUERY_RESULTS_STATUS_EMPTY,
  QUERY_RESULTS_STATUS_SUCCESS,
  PANEL_CONTENTS_WIDTHS,
} from 'src/usage/components/Constants'
import PanelSectionBody from 'src/usage/components/PanelSectionBody'
import BillingStatsPanel from 'src/usage/components/BillingStatsPanel'
import TimeRangeDropdown from './TimeRangeDropdown'

export interface Ranges {
  h24: 'Past 24 Hours'
  d7: 'Past 7 Days'
  d30: 'Past 30 Days'
}

const ranges: Ranges = {
  h24: 'Past 24 Hours',
  d7: 'Past 7 Days',
  d30: 'Past 30 Days',
}

interface Props {
  history: {
    billing_stats: string
    rate_limits: string
    write_mb: string
    execution_sec: string
    storage_gb: string
  }
  limitStatuses: string
  accountType: string
  selectedRange: string
  billingStart: {date: string; time: string}
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
      history.billing_stats
    )

    const {table: limitsTable, status: limitsStatus} = this.csvToTable(
      history.rate_limits
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
          widths={PANEL_CONTENTS_WIDTHS.billing_stats}
        />
        <TimeRangeDropdown
          selectedTimeRange={ranges[selectedRange]}
          dropdownOptions={ranges}
          onSelect={this.handleTimeRangeChange}
        />
        <Panel className="usage--panel">
          <Panel.Header>
            <h4>{`Usage ${ranges[selectedRange]}`}</h4>
            <UsageDropdown
              selectedUsage={selectedUsageID}
              onSelect={this.handleUsageChange}
            />
          </Panel.Header>
          <PanelSection>{this.getUsageSparkline()}</PanelSection>
        </Panel>
        <Panel className="usage--panel">
          <Panel.Header>
            <h5>{`Rate Limits ${ranges[selectedRange]}`}</h5>
          </Panel.Header>
          <PanelSection>
            {GRAPH_INFO.rate_limits.map(graphInfo => {
              return (
                <PanelSectionBody
                  table={limitsTable}
                  status={limitsStatus}
                  graphInfo={graphInfo}
                  widths={PANEL_CONTENTS_WIDTHS.rate_limits}
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
          history.write_mb
        )
        return GRAPH_INFO.write_mb.map(graphInfo => {
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
        } = this.csvToTable(history.execution_sec)
        return GRAPH_INFO.execution_sec.map(graphInfo => {
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
          history.storage_gb
        )
        return GRAPH_INFO.storage_gb.map(graphInfo => {
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

  csvToTable = csv => {
    const {table} = fromFlux(csv)

    if (!table.length) {
      return {
        status: QUERY_RESULTS_STATUS_EMPTY,
        table: {columns: {}, length: 0},
      }
    }

    return {status: QUERY_RESULTS_STATUS_SUCCESS, table}
  }
}

export default UsageToday
