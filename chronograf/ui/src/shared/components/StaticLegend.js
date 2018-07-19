import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import uuid from 'uuid'
import {removeMeasurement} from 'shared/graphs/helpers'
import {ErrorHandling} from 'src/shared/decorators/errors'

const staticLegendItemClassname = (visibilities, i, hoverEnabled) => {
  if (visibilities.length) {
    return `${hoverEnabled ? 'static-legend--item' : 'static-legend--single'}${
      visibilities[i] ? '' : ' disabled'
    }`
  }

  // all series are visible to match expected initial state
  return 'static-legend--item'
}

@ErrorHandling
class StaticLegend extends Component {
  constructor(props) {
    super(props)
  }

  state = {
    visibilities: [],
    clickStatus: false,
  }

  componentDidMount = () => {
    const {height} = this.staticLegendRef.getBoundingClientRect()
    this.props.onUpdateHeight(height)
  }

  componentDidUpdate = prevProps => {
    const {height} = this.staticLegendRef.getBoundingClientRect()

    if (prevProps.height === height) {
      return
    }

    this.props.onUpdateHeight(height)
  }

  componentWillUnmount = () => {
    this.props.onUpdateHeight(0)
  }

  handleClick = i => e => {
    const visibilities = this.props.dygraph.visibility()
    const clickStatus = this.state.clickStatus

    if (e.shiftKey || e.metaKey) {
      visibilities[i] = !visibilities[i]
      this.props.dygraph.setVisibility(visibilities)
      this.setState({visibilities})
      return
    }

    const prevClickStatus = clickStatus && visibilities[i]

    const newVisibilities = prevClickStatus
      ? _.map(visibilities, () => true)
      : _.map(visibilities, () => false)

    newVisibilities[i] = true

    this.props.dygraph.setVisibility(newVisibilities)
    this.setState({
      visibilities: newVisibilities,
      clickStatus: !prevClickStatus,
    })
  }

  render() {
    const {dygraphSeries} = this.props
    const {visibilities} = this.state
    const labels = _.keys(dygraphSeries)
    const colors = _.map(labels, l => dygraphSeries[l].color)

    const hoverEnabled = labels.length > 1

    return (
      <div
        className="static-legend"
        ref={s => {
          this.staticLegendRef = s
        }}
      >
        {_.map(labels, (v, i) => (
          <div
            className={staticLegendItemClassname(visibilities, i, hoverEnabled)}
            key={uuid.v4()}
            onMouseDown={this.handleClick(i)}
          >
            <span style={{color: colors[i]}}>{removeMeasurement(v)}</span>
          </div>
        ))}
      </div>
    )
  }
}

const {shape, func, number} = PropTypes

StaticLegend.propTypes = {
  dygraphSeries: shape({}),
  dygraph: shape({}),
  height: number.isRequired,
  onUpdateHeight: func.isRequired,
}

export default StaticLegend
