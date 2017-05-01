import React, {PropTypes} from 'react'
import classNames from 'classnames'
import _ from 'lodash'

import {showMeasurements} from 'shared/apis/metaQuery'
import showMeasurementsParser from 'shared/parsing/showMeasurements'
import TagList from './TagList'

const {func, shape, string} = PropTypes

const MeasurementList = React.createClass({
  propTypes: {
    query: shape({
      database: string,
      measurement: string,
    }).isRequired,
    onChooseMeasurement: func.isRequired,
    onChooseTag: func.isRequired,
    onToggleTagAcceptance: func.isRequired,
    onGroupByTag: func.isRequired,
  },

  contextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      measurements: [],
      filterText: '',
    }
  },

  componentDidMount() {
    if (!this.props.query.database) {
      return
    }

    this._getMeasurements()
  },

  componentDidUpdate(prevProps) {
    const {query} = this.props

    if (!query.database) {
      return
    }

    if (prevProps.query.database === query.database) {
      return
    }

    this._getMeasurements()
  },

  handleFilterText(e) {
    e.stopPropagation()
    this.setState({
      filterText: this.refs.filterText.value,
    })
  },

  handleEscape(e) {
    if (e.key !== 'Escape') {
      return
    }

    e.stopPropagation()
    this.setState({
      filterText: '',
    })
  },

  handleAcceptReject(e) {
    e.stopPropagation()
    this.props.onToggleTagAcceptance()
  },

  render() {
    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">
          <span>Measurements</span>
          {this.props.query.database
            ? <div className="query-builder--filter">
                <input
                  className="form-control input-sm"
                  ref="filterText"
                  placeholder="Filter"
                  type="text"
                  value={this.state.filterText}
                  onChange={this.handleFilterText}
                  onKeyUp={this.handleEscape}
                />
                <span className="icon search" />
              </div>
            : null}
        </div>
        {this.renderList()}
      </div>
    )
  },

  renderList() {
    if (!this.props.query.database) {
      return (
        <div className="query-builder--list-empty">
          <span>No <strong>Database</strong> selected</span>
        </div>
      )
    }

    const measurements = this.state.measurements.filter(m =>
      m.match(this.state.filterText)
    )

    return (
      <div className="query-builder--list">
        {measurements.map(measurement => {
          const isActive = measurement === this.props.query.measurement
          const numTagsActive = Object.keys(this.props.query.tags).length
          return (
            <div key={measurement}>
              <div
                className={classNames('query-builder--list-item', {active: isActive})}
                onClick={isActive ? _.wrap(null, this.props.onChooseMeasurement) : _.wrap(measurement, this.props.onChooseMeasurement)}
              >
                <span>
                  <div className="query-builder--checkbox"></div>
                  {measurement}
                </span>
                {(isActive && numTagsActive >= 1)
                  ? <div className={classNames('flip-toggle', {flipped: this.props.query.areTagsAccepted})} onClick={this.handleAcceptReject}>
                      <div className="flip-toggle--container">
                        <div className="flip-toggle--front">!=</div>
                        <div className="flip-toggle--back">=</div>
                      </div>
                    </div>
                  : null
                }
              </div>
              {
                isActive ?
                <TagList
                  query={this.props.query}
                  onChooseTag={this.props.onChooseTag}
                  onGroupByTag={this.props.onGroupByTag}
                />
                : null
              }
            </div>
          )
        })}
      </div>
    )
  },

  _getMeasurements() {
    const {source} = this.context
    const proxy = source.links.proxy
    showMeasurements(proxy, this.props.query.database).then(resp => {
      const {errors, measurementSets} = showMeasurementsParser(resp.data)
      if (errors.length) {
        // TODO: display errors in the UI.
        return console.error('InfluxDB returned error(s): ', errors) // eslint-disable-line no-console
      }

      this.setState({
        measurements: measurementSets[0].measurements,
      })
    })
  },
})

export default MeasurementList
