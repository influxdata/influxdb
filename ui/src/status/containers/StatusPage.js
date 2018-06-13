import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import SourceIndicator from 'shared/components/SourceIndicator'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import LayoutRenderer from 'shared/components/LayoutRenderer'

import {fixtureStatusPageCells} from 'src/status/fixtures'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'

@ErrorHandling
class StatusPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      cells: fixtureStatusPageCells,
    }
  }

  render() {
    const {source, autoRefresh, timeRange} = this.props
    const {cells} = this.state

    const dashboardTime = {
      id: 'dashtime',
      tempVar: TEMP_VAR_DASHBOARD_TIME,
      type: 'constant',
      values: [
        {
          value: timeRange.lower,
          type: 'constant',
          selected: true,
        },
      ],
    }

    const upperDashboardTime = {
      id: 'upperdashtime',
      tempVar: TEMP_VAR_UPPER_DASHBOARD_TIME,
      type: 'constant',
      values: [
        {
          value: 'now()',
          type: 'constant',
          selected: true,
        },
      ],
    }

    const templates = [dashboardTime, upperDashboardTime]

    return (
      <div className="page">
        <div className="page-header full-width">
          <div className="page-header--container">
            <div className="page-header--left">
              <h1 className="page-header--title">Status</h1>
            </div>
            <div className="page-header--right">
              <SourceIndicator />
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
          <div className="dashboard container-fluid full-width">
            {cells.length ? (
              <LayoutRenderer
                autoRefresh={autoRefresh}
                timeRange={timeRange}
                cells={cells}
                templates={templates}
                source={source}
                shouldNotBeEditable={true}
                isStatusPage={true}
                isEditable={false}
              />
            ) : (
              <span>Loading Status Page...</span>
            )}
          </div>
        </FancyScrollbar>
      </div>
    )
  }
}

const {number, shape, string} = PropTypes

StatusPage.propTypes = {
  source: shape({
    name: string.isRequired,
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
  autoRefresh: number.isRequired,
  timeRange: shape({
    lower: string.isRequired,
  }).isRequired,
}

const mapStateToProps = ({statusUI: {autoRefresh, timeRange}}) => ({
  autoRefresh,
  timeRange,
})

export default connect(mapStateToProps, null)(StatusPage)
