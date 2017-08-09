import React, {Component, PropTypes} from 'react'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import AxesOptions from 'src/dashboards/components/AxesOptions'

import {buildDefaultYLabel} from 'shared/presenters'

class DisplayOptions extends Component {
  constructor(props) {
    super(props)

    const {axes, queries} = props

    this.state = {
      axes: this.setDefaultLabels(axes, queries),
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.queries.length !== this.props.queries.length) {
      const {axes, queries} = nextProps

      this.setState({axes: this.setDefaultLabels(axes, queries)})
    }
  }

  setDefaultLabels(axes, queryConfigs) {
    if (!queryConfigs.length) {
      return axes
    }

    const defaultYLabel = buildDefaultYLabel(queryConfigs[0])

    return {...axes, y: {...axes.y, defaultYLabel}}
  }

  render() {
    const {
      selectedGraphType,
      onSelectGraphType,
      onSetLabel,
      onSetYAxisBoundMin,
      onSetYAxisBoundMax,
    } = this.props
    const {axes} = this.state

    return (
      <div className="display-options">
        <GraphTypeSelector
          selectedGraphType={selectedGraphType}
          onSelectGraphType={onSelectGraphType}
        />
        <AxesOptions
          onSetLabel={onSetLabel}
          onSetYAxisBoundMin={onSetYAxisBoundMin}
          onSetYAxisBoundMax={onSetYAxisBoundMax}
          axes={axes}
        />
      </div>
    )
  }
}
const {arrayOf, func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
  onSetYAxisBoundMin: func.isRequired,
  onSetYAxisBoundMax: func.isRequired,
  onSetLabel: func.isRequired,
  axes: shape({}).isRequired,
  queries: arrayOf(shape()).isRequired,
}

export default DisplayOptions
