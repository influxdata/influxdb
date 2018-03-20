import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

import {showMeasurements} from 'src/shared/apis/metaQuery'
import showMeasurementsParser from 'src/shared/parsing/showMeasurements'

import {Query, Source} from 'src/types'

import MeasurementListFilter from 'src/shared/components/MeasurementListFilter'
import MeasurementListItem from 'src/shared/components/MeasurementListItem'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

interface Props {
  query: Query
  querySource: Source
  onChooseTag: () => void
  onGroupByTag: () => void
  onToggleTagAcceptance: () => void
  onChooseMeasurement: (measurement: string) => void
}

interface State {
  measurements: string[]
  filterText: string
  filtered: string[]
}

const {shape, string} = PropTypes

class MeasurementList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      filtered: [],
      measurements: [],
      filterText: '',
    }

    this.handleEscape = this.handleEscape.bind(this)
    this.handleFilterText = this.handleFilterText.bind(this)
    this.handleAcceptReject = this.handleAcceptReject.bind(this)
    this.handleFilterMeasuremet = this.handleFilterMeasuremet.bind(this)
    this.handleChoosemeasurement = this.handleChoosemeasurement.bind(this)
  }

  public static defaultProps: Partial<Props> = {
    querySource: null,
  }

  public static contextTypes = {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  }

  componentDidMount() {
    if (!this.props.query.database) {
      return
    }

    this.getMeasurements()
  }

  componentDidUpdate(prevProps) {
    const {query, querySource} = this.props

    if (!query.database) {
      return
    }

    if (
      prevProps.query.database === query.database &&
      _.isEqual(prevProps.querySource, querySource)
    ) {
      return
    }

    this.getMeasurements()
  }

  handleFilterText(e) {
    e.stopPropagation()
    const filterText = e.target.value
    this.setState({
      filterText,
      filtered: this.handleFilterMeasuremet(filterText),
    })
  }

  handleFilterMeasuremet(filter) {
    return this.state.measurements.filter(m =>
      m.toLowerCase().includes(filter.toLowerCase())
    )
  }

  handleEscape(e) {
    if (e.key !== 'Escape') {
      return
    }

    e.stopPropagation()
    this.setState({
      filterText: '',
    })
  }

  handleAcceptReject() {
    this.props.onToggleTagAcceptance()
  }

  handleChoosemeasurement(measurement) {
    return () => this.props.onChooseMeasurement(measurement)
  }

  render() {
    const {query, querySource, onChooseTag, onGroupByTag} = this.props
    const {database, areTagsAccepted} = query
    const {filtered} = this.state

    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">
          <span>Measurements & Tags</span>
          {database &&
            <MeasurementListFilter
              onEscape={this.handleEscape}
              onFilterText={this.handleFilterText}
              filterText={this.state.filterText}
            />}
        </div>
        {database
          ? <div className="query-builder--list">
              <FancyScrollbar>
                {filtered.map(measurement =>
                  <MeasurementListItem
                    query={query}
                    key={measurement}
                    measurement={measurement}
                    querySource={querySource}
                    onChooseTag={onChooseTag}
                    onGroupByTag={onGroupByTag}
                    areTagsAccepted={areTagsAccepted}
                    onAcceptReject={this.handleAcceptReject}
                    isActive={measurement === query.measurement}
                    numTagsActive={Object.keys(query.tags).length}
                    onChooseMeasurement={this.handleChoosemeasurement}
                  />
                )}
              </FancyScrollbar>
            </div>
          : <div className="query-builder--list-empty">
              <span>
                No <strong>Database</strong> selected
              </span>
            </div>}
      </div>
    )
  }

  async getMeasurements() {
    const {source} = this.context
    const {querySource, query} = this.props

    const proxy = _.get(querySource, ['links', 'proxy'], source.links.proxy)

    try {
      const {data} = await showMeasurements(proxy, query.database)
      const {measurementSets} = showMeasurementsParser(data)
      const measurements = measurementSets[0].measurements
      this.setState({measurements, filtered: measurements})
    } catch (err) {
      console.error(err)
    }
  }
}

export default MeasurementList
