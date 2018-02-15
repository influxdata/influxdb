import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import uuid from 'node-uuid'

import {makeLegendStyles} from 'shared/graphs/helpers'

const removeMeasurement = (label = '') => {
  const [measurement] = label.match(/^(.*)[.]/g) || ['']
  return label.replace(measurement, '')
}

class DygraphLegend extends Component {
  state = {
    legend: {
      x: null,
      series: [],
    },
    sortType: '',
    isAscending: true,
    filterText: '',
    isSnipped: false,
    isFilterVisible: false,
    legendStyles: {},
    pageX: null,
  }

  componentDidMount() {
    this.props.dygraph.updateOptions({
      legendFormatter: this.legendFormatter,
      highlightCallback: this.highlightCallback,
      unhighlightCallback: this.unhighlightCallback,
    })
  }

  componentWillUnmount() {
    if (
      !this.props.dygraph.graphDiv ||
      !this.props.dygraph.visibility().find(bool => bool === true)
    ) {
      this.setState({filterText: ''})
    }
  }

  handleToggleFilter = () => {
    this.setState({
      isFilterVisible: !this.state.isFilterVisible,
      filterText: '',
    })
  }

  handleSnipLabel = () => {
    this.setState({isSnipped: !this.state.isSnipped})
  }

  handleLegendInputChange = e => {
    const {dygraph} = this.props
    const {legend} = this.state
    const filterText = e.target.value

    legend.series.map((s, i) => {
      if (!legend.series[i]) {
        return dygraph.setVisibility(i, true)
      }

      dygraph.setVisibility(i, !!legend.series[i].label.match(filterText))
    })

    this.setState({filterText})
  }

  handleSortLegend = sortType => () => {
    this.setState({sortType, isAscending: !this.state.isAscending})
  }

  unhighlightCallback = e => {
    const {
      top,
      bottom,
      left,
      right,
    } = this.legendNodeRef.getBoundingClientRect()

    const mouseY = e.clientY
    const mouseX = e.clientX

    const mouseBuffer = 5
    const mouseInLegendY = mouseY <= bottom && mouseY >= top - mouseBuffer
    const mouseInLegendX = mouseX <= right && mouseX >= left
    const isMouseHoveringLegend = mouseInLegendY && mouseInLegendX

    if (!isMouseHoveringLegend) {
      this.props.onHide(e)
    }
  }

  highlightCallback = ({pageX}) => {
    this.setState({pageX})
    this.props.onShow()
  }

  legendFormatter = legend => {
    if (!legend.x) {
      return ''
    }

    const {legend: prevLegend} = this.state
    const highlighted = legend.series.find(s => s.isHighlighted)
    const prevHighlighted = prevLegend.series.find(s => s.isHighlighted)

    const yVal = highlighted && highlighted.y
    const prevY = prevHighlighted && prevHighlighted.y

    if (legend.x === prevLegend.x && yVal === prevY) {
      return ''
    }

    this.legend = this.setState({legend})
    return ''
  }

  render() {
    const {dygraph, onHide, isHidden} = this.props

    const {
      pageX,
      legend,
      filterText,
      isSnipped,
      sortType,
      isAscending,
      isFilterVisible,
    } = this.state

    const withValues = legend.series.filter(s => !_.isNil(s.y))
    const sorted = _.sortBy(
      withValues,
      ({y, label}) => (sortType === 'numeric' ? y : label)
    )

    const ordered = isAscending ? sorted : sorted.reverse()
    const filtered = ordered.filter(s => s.label.match(filterText))
    const hidden = isHidden ? 'hidden' : ''
    const style = makeLegendStyles(dygraph.graphDiv, this.legendNodeRef, pageX)

    const renderSortAlpha = (
      <div
        className={classnames('sort-btn btn btn-sm btn-square', {
          'btn-primary': sortType !== 'numeric',
          'btn-default': sortType === 'numeric',
          'sort-btn--asc': isAscending && sortType !== 'numeric',
          'sort-btn--desc': !isAscending && sortType !== 'numeric',
        })}
        onClick={this.handleSortLegend('alphabetic')}
      >
        <div className="sort-btn--arrow" />
        <div className="sort-btn--top">A</div>
        <div className="sort-btn--bottom">Z</div>
      </div>
    )

    const renderSortNum = (
      <button
        className={classnames('sort-btn btn btn-sm btn-square', {
          'btn-primary': sortType === 'numeric',
          'btn-default': sortType !== 'numeric',
          'sort-btn--asc': isAscending && sortType === 'numeric',
          'sort-btn--desc': !isAscending && sortType === 'numeric',
        })}
        onClick={this.handleSortLegend('numeric')}
      >
        <div className="sort-btn--arrow" />
        <div className="sort-btn--top">0</div>
        <div className="sort-btn--bottom">9</div>
      </button>
    )
    return (
      <div
        className={`dygraph-legend ${hidden}`}
        ref={el => (this.legendNodeRef = el)}
        onMouseLeave={onHide}
        style={style}
      >
        <div className="dygraph-legend--header">
          <div className="dygraph-legend--timestamp">
            {legend.xHTML}
          </div>
          {renderSortAlpha}
          {renderSortNum}
          <button
            className={classnames('btn btn-square btn-sm', {
              'btn-default': !isFilterVisible,
              'btn-primary': isFilterVisible,
            })}
            onClick={this.handleToggleFilter}
          >
            <span className="icon search" />
          </button>
          <button
            className={classnames('btn btn-sm', {
              'btn-default': !isSnipped,
              'btn-primary': isSnipped,
            })}
            onClick={this.handleSnipLabel}
          >
            Snip
          </button>
        </div>
        {isFilterVisible
          ? <input
              className="dygraph-legend--filter form-control input-sm"
              type="text"
              value={filterText}
              onChange={this.handleLegendInputChange}
              placeholder="Filter items..."
              autoFocus={true}
            />
          : null}
        <div className="dygraph-legend--divider" />
        <div className="dygraph-legend--contents">
          {filtered.map(({label, color, yHTML, isHighlighted}) => {
            const seriesClass = isHighlighted
              ? 'dygraph-legend--row highlight'
              : 'dygraph-legend--row'
            return (
              <div key={uuid.v4()} className={seriesClass}>
                <span style={{color}}>
                  {isSnipped ? removeMeasurement(label) : label}
                </span>
                <figure>
                  {yHTML || 'no value'}
                </figure>
              </div>
            )
          })}
        </div>
      </div>
    )
  }
}

const {bool, func, shape} = PropTypes

DygraphLegend.propTypes = {
  dygraph: shape({}),
  onHide: func.isRequired,
  onShow: func.isRequired,
  isHidden: bool.isRequired,
}

export default DygraphLegend
