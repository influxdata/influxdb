import React, {Component} from 'react'

import SourceIndicator from 'src/shared/components/SourceIndicator'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import LayoutRenderer from 'src/shared/components/LayoutRenderer'
import {STATUS_PAGE_TIME_RANGE} from 'src/shared/data/timeRanges'
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

import {fixtureStatusPageCells} from 'src/status/fixtures'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'
import {Source, Cell} from 'src/types'

interface State {
  cells: Cell[]
}

interface Props {
  source: Source
}

const autoRefresh = AUTOREFRESH_DEFAULT
const timeRange = STATUS_PAGE_TIME_RANGE

@ErrorHandling
class StatusPage extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      cells: fixtureStatusPageCells,
    }
  }

  public render() {
    const {source} = this.props
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
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Status</h1>
            </div>
            <div className="page-header__right">
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

export default StatusPage
