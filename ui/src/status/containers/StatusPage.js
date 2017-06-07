import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import ReactGridLayout, {WidthProvider} from 'react-grid-layout'

import SourceIndicator from 'shared/components/SourceIndicator'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import NameableGraph from 'shared/components/NameableGraph'

const GridLayout = WidthProvider(ReactGridLayout)

const fixtureGraphCells = [
  {
    name: 'Alerts',
    type: 'bar',
    x: 0,
    y: 0,
    w: 12,
    h: 4,
    i: 'bar',
  },
]
const fixtureNonGraphCells = [
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
]

class StatusPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      graphCells: fixtureGraphCells,
      nonGraphCells: fixtureNonGraphCells,
    }

    this.generateLayoutCells = ::this.generateLayoutCells
    this.triggerWindowResize = ::this.triggerWindowResize
  }

  generateNonGraphCell(cell) {
    switch (cell.type) {
      case 'alerts': {
        return (
          <div className="graph-empty">
            <p>Coming soon: Alerts list</p>
          </div>
        )
      }
      case 'news': {
        return (
          <div className="graph-empty">
            <p>Coming soon: Newsfeed</p>
          </div>
        )
      }
      case 'guide': {
        return (
          <div className="graph-empty">
            <p>Coming soon: Markdown-based guide</p>
          </div>
        )
      }
    }
    return (
      <div className="graph-empty">
        <p>No Results</p>
      </div>
    )
  }

  generateLayoutCells(graphCells, nonGraphCells) {
    return [
      ...graphCells.map(cell => {
        return (
          <div key={cell.i}>
            <NameableGraph
              cell={{
                name: cell.name,
                x: cell.x,
                y: cell.y,
              }}
              shouldNotBeEditable={true}
            >
              <RefreshingGraph />
            </NameableGraph>
          </div>
        )
      }),
      ...nonGraphCells.map(cell => {
        return (
          <div key={cell.i}>
            <NameableGraph
              cell={{
                name: cell.name,
                x: cell.x,
                y: cell.y,
              }}
              shouldNotBeEditable={true}
            >
              {this.generateNonGraphCell(cell)}
            </NameableGraph>
          </div>
        )
      }),
    ]
  }

  triggerWindowResize() {
    // Hack to get dygraphs to fit properly during and after resize (dispatchEvent is a global method on window).
    const evt = document.createEvent('CustomEvent') // MUST be 'CustomEvent'
    evt.initCustomEvent('resize', false, false, null)
    dispatchEvent(evt)
  }

  render() {
    const {source} = this.props
    const {graphCells, nonGraphCells} = this.state

    const layoutMargin = 4

    return (
      <div className="page">
        <div className="page-header">
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
            {graphCells.length && nonGraphCells.length
              ? <GridLayout
                  layout={[...graphCells, ...nonGraphCells]}
                  cols={12}
                  rowHeight={83.5}
                  margin={[layoutMargin, layoutMargin]}
                  containerPadding={[0, 0]}
                  useCSSTransforms={false}
                  onResize={this.triggerWindowResize}
                  onLayoutChange={this.handleLayoutChange}
                  draggableHandle={'.dash-graph--name'}
                  isDraggable={false}
                  isResizable={false}
                >
                  {this.generateLayoutCells(graphCells, nonGraphCells)}
                </GridLayout>
              : <span>Loading status...</span>}
          </div>
        </FancyScrollbar>
      </div>
    )
  }
}

const {shape, string} = PropTypes

StatusPage.propTypes = {
  source: shape({
    name: string.isRequired,
  }).isRequired,
}

export default connect(null)(StatusPage)
