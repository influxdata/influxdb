// Libraries
import React, {Component} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import LayoutRenderer from 'src/shared/components/LayoutRenderer'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

// Constants
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'
import {STATUS_PAGE_TIME_RANGE} from 'src/shared/data/timeRanges'
import {fixtureStatusPageCells} from 'src/status/fixtures'
import {
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'

// Types
import {
  Source,
  Template,
  Cell,
  TemplateType,
  TemplateValueType,
} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface State {
  cells: Cell[]
}

interface Props {
  source: Source
  params: {
    sourceID: string
  }
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
    const {cells} = this.state
    const {source} = this.props

    return (
      <div className="page">
        <PageHeader
          titleText="Status"
          fullWidth={true}
          sourceIndicator={true}
        />
        <FancyScrollbar className="page-contents">
          <div className="dashboard container-fluid full-width">
            {cells.length ? (
              <LayoutRenderer
                host=""
                sources={[]}
                cells={cells}
                source={source}
                manualRefresh={0}
                isEditable={false}
                isStatusPage={true}
                timeRange={timeRange}
                templates={this.templates}
                autoRefresh={autoRefresh}
              />
            ) : (
              <span>Loading Status Page...</span>
            )}
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private get templates(): Template[] {
    const dashboardTime = {
      id: 'dashtime',
      tempVar: TEMP_VAR_DASHBOARD_TIME,
      type: TemplateType.Constant,
      label: '',
      values: [
        {
          value: timeRange.lower,
          type: TemplateValueType.Constant,
          selected: true,
          localSelected: true,
        },
      ],
    }

    const upperDashboardTime = {
      id: 'upperdashtime',
      tempVar: TEMP_VAR_UPPER_DASHBOARD_TIME,
      type: TemplateType.Constant,
      label: '',
      values: [
        {
          value: 'now()',
          type: TemplateValueType.Constant,
          selected: true,
          localSelected: true,
        },
      ],
    }

    return [dashboardTime, upperDashboardTime]
  }
}

export default StatusPage
