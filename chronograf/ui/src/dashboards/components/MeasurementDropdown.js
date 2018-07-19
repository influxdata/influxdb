import React, {Component} from 'react'
import PropTypes from 'prop-types'

import Dropdown from 'shared/components/Dropdown'
import {showMeasurements} from 'shared/apis/metaQuery'
import parsers from 'shared/parsing'
import {ErrorHandling} from 'src/shared/decorators/errors'
const {measurements: showMeasurementsParser} = parsers

@ErrorHandling
class MeasurementDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      measurements: [],
    }
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
    const {measurement, onSelectMeasurement, onStartEdit} = this.props
    return (
      <Dropdown
        items={measurements.map(text => ({text}))}
        selected={measurement || 'Select Measurement'}
        onChoose={onSelectMeasurement}
        onClick={onStartEdit}
      />
    )
  }

  _getMeasurements = async () => {
    const {
      measurement,
      database,
      onSelectMeasurement,
      onErrorThrown,
      source: {
        links: {proxy},
      },
    } = this.props

    try {
      const {data} = await showMeasurements(proxy, database)
      const {measurements} = showMeasurementsParser(data)

      this.setState({measurements})
      const selectedMeasurementText = measurements.includes(measurement)
        ? measurement
        : measurements[0] || 'No measurements'
      onSelectMeasurement({text: selectedMeasurementText})
    } catch (error) {
      console.error(error)
      onErrorThrown(error)
    }
  }
}

const {func, shape, string} = PropTypes

MeasurementDropdown.propTypes = {
  database: string.isRequired,
  measurement: string,
  onSelectMeasurement: func.isRequired,
  onStartEdit: func.isRequired,
  onErrorThrown: func.isRequired,
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
}

export default MeasurementDropdown
