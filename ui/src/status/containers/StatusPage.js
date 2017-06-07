import React, {Component, PropTypes} from 'react'
import ReactGridLayout, {WidthProvider} from 'react-grid-layout'

import SourceIndicator from 'shared/components/SourceIndicator'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import DashboardPage from 'src/dashboards/components/DashboardPage'

const fixtureStatusPage = {
  timeRange: {
    upper: null,
    lower: 'now() - 30d',
  },
  dashboard: {
    name: 'Status',
    cells: [
      {
        name: 'Alerts',
        type: 'bar',
        x: 0,
        y: 0,
        w: 12,
        h: 4,
        i: 'bar',
      },
      {
        name: 'Recent Alerts',
        type: 'alerts',
        x: 0,
        y: 5,
        w: 4,
        h: 5,
        i: 'recent-alerts',
      },
      {
        name: 'News Feed',
        type: 'news',
        x: 4,
        y: 5,
        w: 4,
        h: 5,
        i: 'news-feed',
      },
      {
        name: 'Getting Started',
        type: 'guide',
        x: 8,
        y: 5,
        w: 4,
        h: 5,
        i: 'getting-started',
      },
    ],
  },
}

class StatusPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      dashboard: mockStatusPageDashboard,
    }
  }

  render() {
    const {source} = this.props
    const {dashboard, timeRange} = this.state

    return <DashboardPage dashboard={dashboard} />
  }
}

const {shape, string} = PropTypes

StatusPage.propTypes = {
  source: shape({
    name: string.isRequired,
  }).isRequired,
}

export default StatusPage
