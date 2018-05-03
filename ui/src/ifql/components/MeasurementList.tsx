import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'

import {showMeasurements} from 'src/shared/apis/metaQuery'
import showMeasurementsParser from 'src/shared/parsing/showMeasurements'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import MeasurementListFilter from 'src/shared/components/MeasurementListFilter'
import MeasurementListItem from 'src/shared/components/MeasurementListItem'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onChooseMeasurement: (measurement: string) => void
  db: string
}

interface State {
  measurements: string[]
  filterText: string
  filtered: string[]
  selected: string
}

const {shape} = PropTypes

@ErrorHandling
class MeasurementList extends PureComponent<Props, State> {
  public static contextTypes = {
    source: shape({
      links: shape({}).isRequired,
    }).isRequired,
  }

  constructor(props) {
    super(props)
    this.state = {
      filterText: '',
      filtered: [],
      measurements: [],
      selected: '',
    }
  }

  public componentDidMount() {
    if (!this.props.db) {
      return
    }

    this.getMeasurements()
  }

  public render() {
    const {filtered} = this.state

    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">
          <span>Measurements & Tags</span>
          <MeasurementListFilter
            onEscape={this.handleEscape}
            onFilterText={this.handleFilterText}
            filterText={this.state.filterText}
          />
        </div>
        <div className="query-builder--list">
          {filtered.map(measurement => (
            <MeasurementListItem
              key={measurement}
              measurement={measurement}
              selected={this.state.selected}
              onChooseTag={this.handleChooseTag}
              onChooseMeasurement={this.handleChooseMeasurement}
            />
          ))}
        </div>
      </div>
    )
  }

  private handleChooseTag = () => {
    console.log('Choose a tag')
  }

  private async getMeasurements() {
    const {source} = this.context
    const {db} = this.props

    try {
      const {data} = await showMeasurements(source.links.proxy, db)
      const {measurementSets} = showMeasurementsParser(data)
      const measurements = measurementSets[0].measurements

      const selected = measurements[0]
      this.setState({measurements, filtered: measurements, selected})
    } catch (err) {
      console.error(err)
    }
  }

  private handleChooseMeasurement = (selected: string): void => {
    this.setState({selected})
  }

  private handleFilterText = e => {
    e.stopPropagation()
    const filterText = e.target.value
    this.setState({
      filterText,
      filtered: this.handleFilterMeasuremet(filterText),
    })
  }

  private handleFilterMeasuremet = filter => {
    return this.state.measurements.filter(m =>
      m.toLowerCase().includes(filter.toLowerCase())
    )
  }

  private handleEscape = e => {
    if (e.key !== 'Escape') {
      return
    }

    e.stopPropagation()
    this.setState({
      filterText: '',
    })
  }
}

export default MeasurementList
