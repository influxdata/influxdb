import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'

import SourceIndicator from 'shared/components/SourceIndicator'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import LayoutRenderer from 'shared/components/LayoutRenderer'

const fixtureStatusPageCells = [
  {
    i: 'c-bar-graphs-fly',
    isWidget: false,
    x: 0,
    y: 0,
    w: 12,
    h: 4,
    name: 'Alerts – Aspiring Bar Graph',
    queries: [
      {
        query: 'SELECT "usage_user" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime:',
        label: 'cpu.usage_user',
        queryConfig: {
          database: 'telegraf',
          measurement: 'cpu',
          retentionPolicy: 'autogen',
          fields: [
            {
              field: 'usage_user',
              funcs: [],
            },
          ],
          tags: {},
          groupBy: {
            time: '',
            tags: [],
          },
          areTagsAccepted: false,
          rawText: null,
          range: null,
        },
      },
    ],
    type: 'line',
    links: {
      self: '/chronograf/v1/status/23/cells/c-bar-graphs-fly',
    },
  },
  {
    i: 'recent-alerts',
    isWidget: true,
    name: 'Recent Alerts',
    type: 'alerts',
    x: 0,
    y: 5,
    w: 5,
    h: 5,
  },
  {
    i: 'news-feed',
    isWidget: true,
    name: 'News Feed',
    type: 'news',
    x: 5,
    y: 5,
    w: 3.5,
    h: 5,
  },
  {
    i: 'getting-started',
    isWidget: true,
    name: 'Getting Started',
    type: 'guide',
    x: 8.5,
    y: 5,
    w: 3.5,
    h: 5,
  },
]

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
      tempVar: ':dashboardTime:',
      type: 'constant',
      values: [
        {
          value: timeRange.lower,
          type: 'constant',
          selected: true,
        },
      ],
    }
    const templates = [dashboardTime]

    return (
      <div className="page">
        <div className="page-header full-width">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">
                Status
              </h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator sourceName={source.name} />
            </div>
          </div>
        </div>
        <FancyScrollbar className={'page-contents'}>
          <div className="dashboard container-fluid full-width">
            {cells.length
              ? <LayoutRenderer
                  autoRefresh={autoRefresh}
                  timeRange={timeRange}
                  cells={cells}
                  templates={templates}
                  source={source}
                  shouldNotBeEditable={true}
                />
              : <span>Loading Status Page...</span>}
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

const mapStateToProps = ({status: {autoRefresh, timeRange}}) => ({
  autoRefresh,
  timeRange,
})

export default connect(mapStateToProps, null)(StatusPage)
