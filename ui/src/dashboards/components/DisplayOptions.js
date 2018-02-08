import React, {Component, PropTypes} from 'react'

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
      gaugeColors,
      singleStatColors,
      onSetBase,
      onSetScale,
      onSetLabel,
      selectedGraphType,
      onSetPrefixSuffix,
      onSetYAxisBoundMin,
      onSetYAxisBoundMax,
      onAddGaugeThreshold,
      onAddSingleStatThreshold,
      onDeleteThreshold,
      onChooseColor,
      onValidateColorValue,
      onUpdateColorValue,
      singleStatColoration,
      onToggleSingleStatColoration,
      onSetSuffix,
    } = this.props
    const {axes, axes: {y: {suffix}}} = this.state

    switch (selectedGraphType) {
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
        return (
          <SingleStatOptions
            colors={singleStatColors}
            suffix={suffix}
            onSetSuffix={onSetSuffix}
            onChooseColor={onChooseColor}
            onValidateColorValue={onValidateColorValue}
            onUpdateColorValue={onUpdateColorValue}
            onAddThreshold={onAddSingleStatThreshold}
            onDeleteThreshold={onDeleteThreshold}
            singleStatColoration={singleStatColoration}
            onToggleSingleStatColoration={onToggleSingleStatColoration}
          />
        )
      default:
        return (
          <AxesOptions
            selectedGraphType={selectedGraphType}
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
    const {selectedGraphType, onSelectGraphType} = this.props

    return (
      <div className="display-options">
        <GraphTypeSelector
          selectedGraphType={selectedGraphType}
          onSelectGraphType={onSelectGraphType}
        />
        {this.renderOptions()}
      </div>
    )
  }
}
const {arrayOf, func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  onAddGaugeThreshold: func.isRequired,
  onAddSingleStatThreshold: func.isRequired,
  onDeleteThreshold: func.isRequired,
  onChooseColor: func.isRequired,
  onValidateColorValue: func.isRequired,
  onUpdateColorValue: func.isRequired,
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
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
      value: string.isRequired,
    }).isRequired
  ),
  singleStatColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
  queryConfigs: arrayOf(shape()).isRequired,
  singleStatColoration: string.isRequired,
  onToggleSingleStatColoration: func.isRequired,
}

export default DisplayOptions
