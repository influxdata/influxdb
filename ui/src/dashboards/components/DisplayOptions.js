import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import GaugeOptions from 'src/dashboards/components/GaugeOptions'
import SingleStatOptions from 'src/dashboards/components/SingleStatOptions'
import AxesOptions from 'src/dashboards/components/AxesOptions'

import {buildDefaultYLabel} from 'shared/presenters'

class DisplayOptions extends Component {
  constructor(props) {
    super(props)

    const {axes, queryConfigs} = props

    this.state = {
      axes: this.setDefaultLabels(axes, queryConfigs),
    }
  }

  componentWillReceiveProps(nextProps) {
    const {axes, queryConfigs} = nextProps

    this.setState({axes: this.setDefaultLabels(axes, queryConfigs)})
  }

  setDefaultLabels(axes, queryConfigs) {
    return queryConfigs.length
      ? {
          ...axes,
          y: {...axes.y, defaultYLabel: buildDefaultYLabel(queryConfigs[0])},
        }
      : axes
  }

  renderOptions = () => {
    const {cell: {type}, staticLegend, onToggleStaticLegend} = this.props
    switch (type) {
      case 'gauge':
        return <GaugeOptions />
      case 'single-stat':
        return <SingleStatOptions />
      default:
        return (
          <AxesOptions
            onToggleStaticLegend={onToggleStaticLegend}
            staticLegend={staticLegend}
          />
        )
    }
  }

  render() {
    return (
      <div className="display-options">
        <GraphTypeSelector />
        {this.renderOptions()}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  cell: shape({
    type: string.isRequired,
  }).isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
      label: string,
      defaultYLabel: string,
    }),
  }).isRequired,
  queryConfigs: arrayOf(shape()).isRequired,
  onToggleStaticLegend: func.isRequired,
  staticLegend: bool,
}

const mapStateToProps = ({cellEditorOverlay: {cell, cell: {axes}}}) => ({
  cell,
  axes,
})

export default connect(mapStateToProps, null)(DisplayOptions)
