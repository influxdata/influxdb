import React, {PureComponent} from 'react'

import _ from 'lodash'

import {showMeasurements} from 'src/shared/apis/metaQuery'
import showMeasurementsParser from 'src/shared/parsing/showMeasurements'

import {QueryConfig, Source, Tag} from 'src/types'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import MeasurementListFilter from 'src/shared/components/MeasurementListFilter'
import MeasurementListItem from 'src/shared/components/MeasurementListItem'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  query: QueryConfig
  querySource?: Source
  onChooseMeasurement: (measurement: string) => void
  onChooseTag: (tag: Tag) => void
  onGroupByTag: (tagKey: string) => void
  onToggleTagAcceptance: () => void
  isQuerySupportedByExplorer?: boolean
  source: Source
}

interface State {
  measurements: string[]
  filterText: string
  filtered: string[]
}

@ErrorHandling
class MeasurementList extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    querySource: null,
  }

  constructor(props) {
    super(props)
    this.state = {
      filterText: '',
      filtered: [],
      measurements: [],
    }

    this.handleEscape = this.handleEscape.bind(this)
    this.handleFilterText = this.handleFilterText.bind(this)
    this.handleAcceptReject = this.handleAcceptReject.bind(this)
    this.handleFilterMeasuremet = this.handleFilterMeasuremet.bind(this)
    this.handleChoosemeasurement = this.handleChoosemeasurement.bind(this)
  }

  public componentDidMount() {
    if (!this.props.query.database) {
      return
    }

    this.getMeasurements()
  }

  public componentDidUpdate(prevProps) {
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

  public handleFilterText(e) {
    e.stopPropagation()
    const filterText = e.target.value
    this.setState({
      filterText,
      filtered: this.handleFilterMeasuremet(filterText),
    })
  }

  public handleFilterMeasuremet(filter) {
    return this.state.measurements.filter(m =>
      m.toLowerCase().includes(filter.toLowerCase())
    )
  }

  public handleEscape(e) {
    if (e.key !== 'Escape') {
      return
    }

    e.stopPropagation()
    this.setState({
      filterText: '',
    })
  }

  public handleAcceptReject() {
    this.props.onToggleTagAcceptance()
  }

  public handleChoosemeasurement(measurement) {
    return () => this.props.onChooseMeasurement(measurement)
  }

  public render() {
    const {
      query,
      querySource,
      onChooseTag,
      onGroupByTag,
      isQuerySupportedByExplorer,
    } = this.props
    const {database, areTagsAccepted} = query
    const {filtered} = this.state

    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">
          <span>Measurements & Tags</span>
          {database && (
            <MeasurementListFilter
              onEscape={this.handleEscape}
              onFilterText={this.handleFilterText}
              filterText={this.state.filterText}
            />
          )}
        </div>
        {database ? (
          <div className="query-builder--list">
            <FancyScrollbar>
              {filtered.map(measurement => (
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
                  isQuerySupportedByExplorer={isQuerySupportedByExplorer}
                  numTagsActive={Object.keys(query.tags).length}
                  onChooseMeasurement={this.handleChoosemeasurement}
                />
              ))}
            </FancyScrollbar>
          </div>
        ) : (
          <div className="query-builder--list-empty">
            <span>
              No <strong>Database</strong> selected
            </span>
          </div>
        )}
      </div>
    )
  }

  public async getMeasurements() {
    const {querySource, query, source} = this.props

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
