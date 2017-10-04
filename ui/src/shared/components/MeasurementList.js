import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {showMeasurements} from 'shared/apis/metaQuery'
import showMeasurementsParser from 'shared/parsing/showMeasurements'

import TagList from 'src/data_explorer/components/TagList'
import FancyScrollbar from 'shared/components/FancyScrollbar'

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
    querySource: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }),
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

  getDefaultProps() {
    return {
      querySource: null,
    }
  },

  componentDidMount() {
    if (!this.props.query.database) {
      return
    }

    this._getMeasurements()
  },

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
          <span>Measurements & Tags</span>
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
                  spellCheck={false}
                  autoComplete={false}
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
          <span>
            No <strong>Database</strong> selected
          </span>
        </div>
      )
    }

    const filterText = this.state.filterText.toLowerCase()
    const measurements = this.state.measurements.filter(m =>
      m.toLowerCase().includes(filterText)
    )

    return (
      <div className="query-builder--list">
        <FancyScrollbar>
          {measurements.map(measurement => {
            const isActive = measurement === this.props.query.measurement
            const numTagsActive = Object.keys(this.props.query.tags).length

            return (
              <div
                key={measurement}
                onClick={
                  isActive
                    ? () => {}
                    : () => this.props.onChooseMeasurement(measurement)
                }
              >
                <div
                  className={classnames('query-builder--list-item', {
                    active: isActive,
                  })}
                  data-test={`query-builder-list-item-measurement-${measurement}`}
                >
                  <span>
                    <div className="query-builder--caret icon caret-right" />
                    {measurement}
                  </span>
                  {isActive && numTagsActive >= 1
                    ? <div
                        className={classnames('flip-toggle', {
                          flipped: this.props.query.areTagsAccepted,
                        })}
                        onClick={this.handleAcceptReject}
                      >
                        <div className="flip-toggle--container">
                          <div className="flip-toggle--front">!=</div>
                          <div className="flip-toggle--back">=</div>
                        </div>
                      </div>
                    : null}
                </div>
                {isActive
                  ? <TagList
                      query={this.props.query}
                      onChooseTag={this.props.onChooseTag}
                      onGroupByTag={this.props.onGroupByTag}
                    />
                  : null}
              </div>
            )
          })}
        </FancyScrollbar>
      </div>
    )
  },

  _getMeasurements() {
    const {source} = this.context
    const {querySource} = this.props

    const proxy =
      _.get(querySource, ['links', 'proxy'], null) || source.links.proxy

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
