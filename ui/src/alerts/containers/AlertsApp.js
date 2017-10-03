import React, {PropTypes, Component} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'
import AlertsTable from 'src/alerts/components/AlertsTable'
import NoKapacitorError from 'shared/components/NoKapacitorError'
import CustomTimeRangeDropdown from 'shared/components/CustomTimeRangeDropdown'

import {getAlerts} from 'src/alerts/apis'
import AJAX from 'utils/ajax'

import _ from 'lodash'
import moment from 'moment'

import timeRanges from 'hson!shared/data/timeRanges.hson'

class AlertsApp extends Component {
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
        lower: moment().subtract(lowerInSec || oneDayInSec, 'seconds').format(),
      },
      limit: props.limit || 0, // only used if AlertsApp receives a limit prop
      limitMultiplier: 1, // only used if AlertsApp receives a limit prop
      isAlertsMaxedOut: false, // only used if AlertsApp receives a limit prop
    }

    this.fetchAlerts = ::this.fetchAlerts
    this.renderSubComponents = ::this.renderSubComponents
    this.handleGetMoreAlerts = ::this.handleGetMoreAlerts
    this.handleApplyTime = ::this.handleApplyTime
  }

  // TODO: show a loading screen until we figure out if there is a kapacitor and fetch the alerts
  componentDidMount() {
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

  componentDidUpdate(prevProps, prevState) {
    if (!_.isEqual(prevState.timeRange, this.state.timeRange)) {
      this.fetchAlerts()
    }
  }

  fetchAlerts() {
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

  handleGetMoreAlerts() {
    this.setState({limitMultiplier: this.state.limitMultiplier + 1}, () => {
      this.fetchAlerts(this.state.limitMultiplier)
    })
  }

  renderSubComponents() {
    const {source, isWidget, limit} = this.props
    const {isAlertsMaxedOut, alerts} = this.state

    return this.state.hasKapacitor
      ? <AlertsTable
          source={source}
          alerts={this.state.alerts}
          shouldNotBeFilterable={isWidget}
          limit={limit}
          onGetMoreAlerts={this.handleGetMoreAlerts}
          isAlertsMaxedOut={isAlertsMaxedOut}
          alertsCount={alerts.length}
        />
      : <NoKapacitorError source={source} />
  }

  handleApplyTime(timeRange) {
    this.setState({timeRange})
  }

  render() {
    const {isWidget, source} = this.props
    const {loading, timeRange} = this.state

    if (loading || !source) {
      return <div className="page-spinner" />
    }

    return isWidget
      ? this.renderSubComponents()
      : <div className="page alert-history-page">
          <div className="page-header">
            <div className="page-header__container">
              <div className="page-header__left">
                <h1 className="page-header__title">Alert History</h1>
              </div>
              <div className="page-header__right">
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
                <div className="col-md-12">
                  {this.renderSubComponents()}
                </div>
              </div>
            </div>
          </div>
        </div>
  }
}

const {bool, number, oneOfType, shape, string} = PropTypes

AlertsApp.propTypes = {
  source: shape({
    id: string.isRequired,
    name: string.isRequired,
    type: string, // 'influx-enterprise'
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }),
  timeRange: shape({
    lower: string.isRequired,
    upper: oneOfType([shape(), string]),
  }),
  isWidget: bool,
  limit: number,
}

export default AlertsApp
