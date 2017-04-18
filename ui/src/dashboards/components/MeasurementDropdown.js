import React, {PropTypes, Component} from 'react'
import Dropdown from 'shared/components/Dropdown'

import {showMeasurements} from 'shared/apis/metaQuery'
import showMeasurementsParser from 'shared/parsing/showMeasurements'

class MeasurementDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      measurements: [],
    }

    this._getMeasurements = ::this._getMeasurements
  }

  componentDidMount() {
    this._getMeasurements()
  }

  componentDidUpdate(nextProps) {
    if (nextProps.database === this.props.database) {
      return
    }

    this._getMeasurements()
  }

  render() {
    const {measurements} = this.state
    const {measurement, onSelectMeasurement} = this.props
    return (
      <Dropdown
        items={measurements.map(text => ({text}))}
        selected={measurement || 'Select Measurement'}
        onChoose={onSelectMeasurement}
      />
    )
  }

  async _getMeasurements() {
    const {source: {links: {proxy}}} = this.context

    try {
      const {data} = await showMeasurements(proxy, this.props.database)
      const {measurementSets} = showMeasurementsParser(data)
      this.setState({measurements: measurementSets[0].measurements})
    } catch (error) {
      console.error(error)
    }
  }
}

const {func, shape, string} = PropTypes

MeasurementDropdown.contextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
}

MeasurementDropdown.propTypes = {
  database: string.isRequired,
  measurement: string,
  onSelectMeasurement: func.isRequired,
}

export default MeasurementDropdown
