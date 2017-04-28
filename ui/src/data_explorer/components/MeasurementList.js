import React, {PropTypes} from 'react'
import classNames from 'classnames'
import _ from 'lodash'

import {showMeasurements} from 'shared/apis/metaQuery'
import showMeasurementsParser from 'shared/parsing/showMeasurements'

const MeasurementList = React.createClass({
  propTypes: {
    query: PropTypes.shape({
      database: PropTypes.string,
      measurement: PropTypes.string,
    }).isRequired,
    onChooseMeasurement: PropTypes.func.isRequired,
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
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
          return (
            <div
              className={classNames('query-builder--list-item', {
                active: isActive,
              })}
              key={measurement}
              onClick={_.wrap(measurement, this.props.onChooseMeasurement)}
            >
              {measurement}
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
