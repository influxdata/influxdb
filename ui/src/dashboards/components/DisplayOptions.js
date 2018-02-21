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
    const {
      cell,
      onSetBase,
      onSetScale,
      onSetLabel,
      onSetPrefixSuffix,
      onSetYAxisBoundMin,
      onSetYAxisBoundMax,
      onSetSuffix,
    } = this.props
    const {axes, axes: {y: {suffix}}} = this.state

    switch (cell.type) {
      case 'gauge':
        return <GaugeOptions />
      case 'single-stat':
        return <SingleStatOptions suffix={suffix} onSetSuffix={onSetSuffix} />
      default:
        return (
          <AxesOptions
            cellType={cell.type}
            axes={axes}
            onSetBase={onSetBase}
            onSetLabel={onSetLabel}
            onSetScale={onSetScale}
            onSetPrefixSuffix={onSetPrefixSuffix}
            onSetYAxisBoundMin={onSetYAxisBoundMin}
            onSetYAxisBoundMax={onSetYAxisBoundMax}
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
const {arrayOf, func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  cell: shape({
    type: string.isRequired,
  }).isRequired,
  onSetPrefixSuffix: func.isRequired,
  onSetSuffix: func.isRequired,
  onSetYAxisBoundMin: func.isRequired,
  onSetYAxisBoundMax: func.isRequired,
  onSetScale: func.isRequired,
  onSetLabel: func.isRequired,
  onSetBase: func.isRequired,
  axes: shape({}).isRequired,
  queryConfigs: arrayOf(shape()).isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {cell}}) => ({
  cell,
})

export default connect(mapStateToProps, null)(DisplayOptions)
