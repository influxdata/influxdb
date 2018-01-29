import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import uuid from 'node-uuid'

const style = {
  position: 'absolute',
  width: 'calc(100% - 32px)',
  bottom: '8px',
  left: '16px',
  height: '30px',
}

const removeMeasurement = (label = '') => {
  const [measurement] = label.match(/^(.*)[.]/g) || ['']
  return label.replace(measurement, '')
}

const persistentLegendItemClassname = (visibilities, i) => {
  if (visibilities.length) {
    return `persistent-legend--item${visibilities[i] ? '' : ' disabled'}`
  }

  // all series are visible to match expected initial state
  return 'persistent-legend--item'
}

class PersistentLegend extends Component {
  constructor(props) {
    super(props)

    this.state = {
      visibilities: [],
    }
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
      : _.map(visibilities, v => false)
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
      <div className="persistent-legend" style={style}>
        {_.map(labels, (v, i) =>
          <div
            className={persistentLegendItemClassname(visibilities, i)}
            key={uuid.v4()}
            onClick={this.handleClick(i)}
          >
            <div
              className="persistent-legend--dot"
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

const {shape} = PropTypes

PersistentLegend.propTypes = {sharedLegend: shape({}), dygraph: shape({})}

export default PersistentLegend
