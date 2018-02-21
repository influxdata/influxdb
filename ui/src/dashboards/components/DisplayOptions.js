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
      gaugeColors,
      onSetBase,
      onSetScale,
      onSetLabel,
      onSetPrefixSuffix,
      onSetYAxisBoundMin,
      onSetYAxisBoundMax,
      onAddGaugeThreshold,
      onDeleteThreshold,
      onChooseColor,
      onValidateColorValue,
      onUpdateColorValue,
      onSetSuffix,
    } = this.props
    const {axes, axes: {y: {suffix}}} = this.state

    switch (cell.type) {
      case 'gauge':
        return (
          <GaugeOptions
            colors={gaugeColors}
            onChooseColor={onChooseColor}
            onValidateColorValue={onValidateColorValue}
            onUpdateColorValue={onUpdateColorValue}
            onAddThreshold={onAddGaugeThreshold}
            onDeleteThreshold={onDeleteThreshold}
          />
        )
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
const {arrayOf, func, number, shape, string} = PropTypes

DisplayOptions.propTypes = {
  onAddGaugeThreshold: func.isRequired,
  onDeleteThreshold: func.isRequired,
  onChooseColor: func.isRequired,
  onValidateColorValue: func.isRequired,
  onUpdateColorValue: func.isRequired,
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
  gaugeColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: number.isRequired,
    }).isRequired
  ),
  queryConfigs: arrayOf(shape()).isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {cell}}) => ({
  cell,
})

export default connect(mapStateToProps, null)(DisplayOptions)
