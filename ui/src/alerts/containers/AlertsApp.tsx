import React, {PureComponent} from 'react'

import SourceIndicator from 'src/shared/components/SourceIndicator'
import AlertsTable from 'src/alerts/components/AlertsTable'
import NoKapacitorError from 'src/shared/components/NoKapacitorError'
import CustomTimeRangeDropdown from 'src/shared/components/CustomTimeRangeDropdown'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {getAlerts} from 'src/alerts/apis'
import AJAX from 'src/utils/ajax'

import _ from 'lodash'
import moment from 'moment'

import {timeRanges} from 'src/shared/data/timeRanges'

import {Source, TimeRange} from 'src/types'
import {Alert} from '../../types/alerts'

interface Props {
  source: Source
  timeRange: TimeRange
  isWidget: boolean
  limit: number
}

interface State {
  loading: boolean
  hasKapacitor: boolean
  alerts: Alert[]
  timeRange: TimeRange
  limit: number
  limitMultiplier: number
  isAlertsMaxedOut: boolean
}

@ErrorHandling
class AlertsApp extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    const lowerInSec = props.timeRange
      ? timeRanges.find(tr => tr.lower === props.timeRange.lower).seconds
      : undefined

    const oneDayInSec = 86400

    this.state = {
      loading: true,
      hasKapacitor: false,
      alerts: [],
      timeRange: {
        upper: moment().format(),
        lower: moment()
          .subtract(lowerInSec || oneDayInSec, 'seconds')
          .format(),
      },
      limit: props.limit || 0, // only used if AlertsApp receives a limit prop
      limitMultiplier: 1, // only used if AlertsApp receives a limit prop
      isAlertsMaxedOut: false, // only used if AlertsApp receives a limit prop
    }
  }

  // TODO: show a loading screen until we figure out if there is a kapacitor and fetch the alerts
  public componentDidMount() {
    const {source} = this.props
    AJAX({
      url: source.links.kapacitors,
      method: 'GET',
    }).then(({data}) => {
      if (data.kapacitors[0]) {
        this.setState({hasKapacitor: true})

        this.fetchAlerts()
      } else {
        this.setState({loading: false})
      }
    })
  }

  public componentDidUpdate(__, prevState) {
    if (!_.isEqual(prevState.timeRange, this.state.timeRange)) {
      this.fetchAlerts()
    }
  }
  public render() {
    const {isWidget, source} = this.props
    const {loading, timeRange} = this.state

    if (loading || !source) {
      return <div className="page-spinner" />
    }

    return isWidget ? (
      this.renderSubComponents()
    ) : (
      <div className="page alert-history-page">
        <div className="page-header">
          <div className="page-header--container">
            <div className="page-header--left">
              <h1 className="page-header--title">Alert History</h1>
            </div>
            <div className="page-header--right">
              <SourceIndicator />
              <CustomTimeRangeDropdown
                onApplyTimeRange={this.handleApplyTime}
                timeRange={timeRange}
              />
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">{this.renderSubComponents()}</div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  private fetchAlerts = (): void => {
    getAlerts(
      this.props.source.links.proxy,
      this.state.timeRange,
      this.state.limit * this.state.limitMultiplier
    ).then(resp => {
      const results = []

      const alertSeries = _.get(resp, ['data', 'results', '0', 'series'], [])
      if (alertSeries.length === 0) {
        this.setState({loading: false, alerts: []})
        return
      }

      const timeIndex = alertSeries[0].columns.findIndex(col => col === 'time')
      const hostIndex = alertSeries[0].columns.findIndex(col => col === 'host')
      const valueIndex = alertSeries[0].columns.findIndex(
        col => col === 'value'
      )
      const levelIndex = alertSeries[0].columns.findIndex(
        col => col === 'level'
      )
      const nameIndex = alertSeries[0].columns.findIndex(
        col => col === 'alertName'
      )

      alertSeries[0].values.forEach(s => {
        results.push({
          time: `${s[timeIndex]}`,
          host: s[hostIndex],
          value: `${s[valueIndex]}`,
          level: s[levelIndex],
          name: `${s[nameIndex]}`,
        })
      })

      // TODO: factor these setStates out to make a pure function and implement true limit & offset
      this.setState({
        loading: false,
        alerts: results,
        // this.state.alerts.length === results.length ||
        isAlertsMaxedOut:
          results.length !== this.props.limit * this.state.limitMultiplier,
      })
    })
  }

  private handleGetMoreAlerts = (): void => {
    this.setState({limitMultiplier: this.state.limitMultiplier + 1}, () => {
      this.fetchAlerts()
    })
  }

  private renderSubComponents = (): JSX.Element => {
    const {source, isWidget, limit} = this.props
    const {isAlertsMaxedOut, alerts} = this.state

    return this.state.hasKapacitor ? (
      <AlertsTable
        source={source}
        alerts={this.state.alerts}
        shouldNotBeFilterable={isWidget}
        limit={limit}
        onGetMoreAlerts={this.handleGetMoreAlerts}
        isAlertsMaxedOut={isAlertsMaxedOut}
        alertsCount={alerts.length}
      />
    ) : (
      <NoKapacitorError source={source} />
    )
  }

  private handleApplyTime = (timeRange: TimeRange): void => {
    this.setState({timeRange})
  }
}

export default AlertsApp
