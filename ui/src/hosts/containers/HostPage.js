import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import _ from 'lodash'
import classnames from 'classnames'

import Dygraph from 'src/external/dygraph'

import LayoutRenderer from 'shared/components/LayoutRenderer'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import timeRanges from 'hson!shared/data/timeRanges.hson'
import {
  getMappings,
  getAppsForHosts,
  getMeasurementsForHost,
  getAllHosts,
} from 'src/hosts/apis'
import {fetchLayouts} from 'shared/apis'

import {setAutoRefresh} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'

const {shape, string, bool, func, number} = PropTypes

export const HostPage = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
      telegraf: string.isRequired,
      id: string.isRequired,
    }),
    params: shape({
      hostID: string.isRequired,
    }).isRequired,
    location: shape({
      query: shape({
        app: string,
      }),
    }),
    autoRefresh: number.isRequired,
    handleChooseAutoRefresh: func.isRequired,
    inPresentationMode: bool,
    handleClickPresentationButton: func,
  },

  getInitialState() {
    return {
      layouts: [],
      hosts: [],
      timeRange: timeRanges.find(tr => tr.lower === 'now() - 1h'),
      dygraphs: [],
    }
  },

  async componentDidMount() {
    const {source, params, location} = this.props

    // fetching layouts and mappings can be done at the same time
    const {data: {layouts}} = await fetchLayouts()
    const {data: {mappings}} = await getMappings()
    const hosts = await getAllHosts(source.links.proxy, source.telegraf)
    const newHosts = await getAppsForHosts(
      source.links.proxy,
      hosts,
      mappings,
      source.telegraf
    )
    const measurements = await getMeasurementsForHost(source, params.hostID)

    const host = newHosts[this.props.params.hostID]
    const focusedApp = location.query.app

    const filteredLayouts = layouts.filter(layout => {
      if (focusedApp) {
        return layout.app === focusedApp
      }

      return (
        host.apps &&
        host.apps.includes(layout.app) &&
        measurements.includes(layout.measurement)
      )
    })

    // only display hosts in the list if they match the current app
    let filteredHosts = hosts
    if (focusedApp) {
      filteredHosts = _.pickBy(hosts, (val, __, ___) => {
        return val.apps.includes(focusedApp)
      })
    }

    this.setState({layouts: filteredLayouts, hosts: filteredHosts}) // eslint-disable-line react/no-did-mount-set-state
  },

  handleChooseTimeRange({lower, upper}) {
    if (upper) {
      this.setState({timeRange: {lower, upper}})
    } else {
      const timeRange = timeRanges.find(range => range.lower === lower)
      this.setState({timeRange})
    }
  },

  synchronizer(dygraph) {
    const dygraphs = [...this.state.dygraphs, dygraph]
    const numGraphs = this.state.layouts.reduce((acc, {cells}) => {
      return acc + cells.length
    }, 0)

    if (dygraphs.length === numGraphs) {
      Dygraph.synchronize(dygraphs, {
        selection: true,
        zoom: false,
        range: false,
      })
    }
    this.setState({dygraphs})
  },

  renderLayouts(layouts) {
    const {timeRange} = this.state
    const {source, autoRefresh} = this.props

    const autoflowLayouts = layouts.filter(layout => !!layout.autoflow)

    const cellWidth = 4
    const cellHeight = 4
    const pageWidth = 12

    let cellCount = 0
    const autoflowCells = autoflowLayouts.reduce((allCells, layout) => {
      return allCells.concat(
        layout.cells.map(cell => {
          const x = cellCount * cellWidth % pageWidth
          const y = Math.floor(cellCount * cellWidth / pageWidth) * cellHeight
          cellCount += 1
          return Object.assign(cell, {
            w: cellWidth,
            h: cellHeight,
            x,
            y,
          })
        })
      )
    }, [])

    const staticLayouts = layouts.filter(layout => !layout.autoflow)
    staticLayouts.unshift({cells: autoflowCells})

    let translateY = 0
    const layoutCells = staticLayouts.reduce((allCells, layout) => {
      let maxY = 0
      layout.cells.forEach(cell => {
        cell.y += translateY
        if (cell.y > translateY) {
          maxY = cell.y
        }
        cell.queries.forEach(q => {
          q.text = q.query
          q.database = source.telegraf
        })
      })
      translateY = maxY

      return allCells.concat(layout.cells)
    }, [])

    return (
      <LayoutRenderer
        timeRange={timeRange}
        cells={layoutCells}
        autoRefresh={autoRefresh}
        source={source}
        host={this.props.params.hostID}
        isEditable={false}
        synchronizer={this.synchronizer}
      />
    )
  },

  render() {
    const {
      params: {hostID},
      location: {query: {app}},
      source: {id},
      autoRefresh,
      handleChooseAutoRefresh,
      inPresentationMode,
      handleClickPresentationButton,
      source,
    } = this.props
    const {layouts, timeRange, hosts} = this.state
    const appParam = app ? `?app=${app}` : ''

    return (
      <div className="page">
        <DashboardHeader
          buttonText={hostID}
          autoRefresh={autoRefresh}
          timeRange={timeRange}
          isHidden={inPresentationMode}
          handleChooseTimeRange={this.handleChooseTimeRange}
          handleChooseAutoRefresh={handleChooseAutoRefresh}
          handleClickPresentationButton={handleClickPresentationButton}
          source={source}
        >
          {Object.keys(hosts).map((host, i) => {
            return (
              <li className="dropdown-item" key={i}>
                <Link to={`/sources/${id}/hosts/${host + appParam}`}>
                  {host}
                </Link>
              </li>
            )
          })}
        </DashboardHeader>
        <FancyScrollbar
          className={classnames({
            'page-contents': true,
            'presentation-mode': inPresentationMode,
          })}
        >
          <div className="container-fluid full-width dashboard">
            {layouts.length > 0 ? this.renderLayouts(layouts) : ''}
          </div>
        </FancyScrollbar>
      </div>
    )
  },
})

const mapStateToProps = ({
  app: {ephemeral: {inPresentationMode}, persisted: {autoRefresh}},
}) => ({
  inPresentationMode,
  autoRefresh,
})

const mapDispatchToProps = dispatch => ({
  handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(HostPage)
