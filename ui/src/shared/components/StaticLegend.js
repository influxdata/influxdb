import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import uuid from 'node-uuid'

const style = {
  position: 'absolute',
  width: 'calc(100% - 32px)',
  bottom: '8px',
  left: '16px',
  paddingTop: '8px',
}

const removeMeasurement = (label = '') => {
  const [measurement] = label.match(/^(.*)[.]/g) || ['']
  return label.replace(measurement, '')
}

const staticLegendItemClassname = (visibilities, i) => {
  if (visibilities.length) {
    return `static-legend--item${visibilities[i] ? '' : ' disabled'}`
  }

  // all series are visible to match expected initial state
  return 'static-legend--item'
}

class StaticLegend extends Component {
  constructor(props) {
    super(props)

    this.state = {
      visibilities: [],
    }
  }

  componentDidUpdate = () => {
    const {height} = this.staticLegendRef.getBoundingClientRect()
    this.props.handleReceiveStaticLegendHeight(height)
  }

  componentWillUnmount = () => {
    this.props.handleReceiveStaticLegendHeight(null)
  }

  handleClick = i => e => {
    const visibilities = this.props.dygraph.visibility()

    if (e.shiftKey || e.metaKey) {
      visibilities[i] = !visibilities[i]
      this.props.dygraph.setVisibility(visibilities)
      this.setState({visibilities})
      return
    }

    const newVisibilities = visibilities[i]
      ? _.map(visibilities, v => !v)
      : _.map(visibilities, () => false)
    newVisibilities[i] = true

    this.props.dygraph.setVisibility(newVisibilities)
    this.setState({visibilities: newVisibilities})
  }

  render() {
    const {dygraph} = this.props
    const {visibilities} = this.state

    const labels = dygraph ? _.drop(dygraph.getLabels()) : []
    const colors = dygraph
      ? _.map(labels, l => {
          return dygraph.attributes_.series_[l].options.color
        })
      : []

    return (
      <div
        className="static-legend"
        style={style}
        ref={s => {
          this.staticLegendRef = s
        }}
      >
        {_.map(labels, (v, i) =>
          <div
            className={staticLegendItemClassname(visibilities, i)}
            key={uuid.v4()}
            onClick={this.handleClick(i)}
          >
            <div
              className="static-legend--dot"
              style={{backgroundColor: colors[i]}}
            />
            <span style={{color: colors[i]}}>
              {removeMeasurement(v)}
            </span>
          </div>
        )}
      </div>
    )
  }
}

const {shape, func} = PropTypes

StaticLegend.propTypes = {
  sharedLegend: shape({}),
  dygraph: shape({}),
  handleReceiveStaticLegendHeight: func.isRequired,
}

export default StaticLegend
