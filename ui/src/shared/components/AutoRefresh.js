import React, {PropTypes} from 'react'
import _ from 'lodash'
import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'
import {removeUnselectedTemplateValues} from 'src/dashboards/constants'

const {
  array,
  arrayOf,
  bool,
  element,
  func,
  number,
  oneOfType,
  shape,
  string,
} = PropTypes

const AutoRefresh = ComposedComponent => {
  const wrapper = React.createClass({
    propTypes: {
      children: element,
      autoRefresh: number.isRequired,
      templates: arrayOf(
        shape({
          type: string.isRequired,
          tempVar: string.isRequired,
          query: shape({
            db: string,
            rp: string,
            influxql: string,
          }),
          values: arrayOf(
            shape({
              type: string.isRequired,
              value: string.isRequired,
              selected: bool,
            })
          ).isRequired,
        })
      ),
      queries: arrayOf(
        shape({
          host: oneOfType([string, arrayOf(string)]),
          text: string,
        }).isRequired
      ).isRequired,
      axes: shape({
        bounds: shape({
          y: array,
          y2: array,
        }),
      }),
      editQueryStatus: func,
    },

    getInitialState() {
      return {
        lastQuerySuccessful: false,
        timeSeries: [],
        resolution: null,
      }
    },

    componentDidMount() {
      const {queries, templates, autoRefresh} = this.props
      this.executeQueries(queries, templates)
      if (autoRefresh) {
        this.intervalID = setInterval(
          () => this.executeQueries(queries, templates),
          autoRefresh
        )
      }
    },

    componentWillReceiveProps(nextProps) {
      const queriesDidUpdate = this.queryDifference(
        this.props.queries,
        nextProps.queries
      ).length

      const tempVarsDidUpdate = !_.isEqual(
        this.props.templates,
        nextProps.templates
      )

      const shouldRefetch = queriesDidUpdate || tempVarsDidUpdate

      if (shouldRefetch) {
        this.executeQueries(nextProps.queries, nextProps.templates)
      }

      if (this.props.autoRefresh !== nextProps.autoRefresh || shouldRefetch) {
        clearInterval(this.intervalID)

        if (nextProps.autoRefresh) {
          this.intervalID = setInterval(
            () => this.executeQueries(nextProps.queries, nextProps.templates),
            nextProps.autoRefresh
          )
        }
      }
    },

    queryDifference(left, right) {
      const leftStrs = left.map(q => `${q.host}${q.text}`)
      const rightStrs = right.map(q => `${q.host}${q.text}`)
      return _.difference(
        _.union(leftStrs, rightStrs),
        _.intersection(leftStrs, rightStrs)
      )
    },

    executeQueries(queries, templates = []) {
      const {editQueryStatus} = this.props
      const {resolution} = this.state

      if (!queries.length) {
        this.setState({timeSeries: []})
        return
      }

      this.setState({isFetching: true})

      const timeSeriesPromises = queries.map(query => {
        const {host, database, rp} = query

        const templatesWithResolution = templates.map(temp => {
          if (temp.tempVar === ':interval:') {
            if (resolution) {
              return {...temp, resolution}
            }
            return {...temp, resolution: 1000}
          }
          return {...temp}
        })

        return fetchTimeSeriesAsync(
          {
            source: host,
            db: database,
            rp,
            query,
            tempVars: removeUnselectedTemplateValues(templatesWithResolution),
            resolution,
          },
          editQueryStatus
        )
      })

      Promise.all(timeSeriesPromises).then(timeSeries => {
        const newSeries = timeSeries.map(response => ({response}))
        const lastQuerySuccessful = !this._noResultsForQuery(newSeries)

        this.setState({
          timeSeries: newSeries,
          lastQuerySuccessful,
          isFetching: false,
        })
      })
    },

    componentWillUnmount() {
      clearInterval(this.intervalID)
      this.intervalID = false
    },

    setResolution(resolution) {
      this.setState({resolution})
    },

    render() {
      const {timeSeries} = this.state

      if (this.state.isFetching && this.state.lastQuerySuccessful) {
        return this.renderFetching(timeSeries)
      }

      if (
        this._noResultsForQuery(timeSeries) ||
        !this.state.lastQuerySuccessful
      ) {
        return this.renderNoResults()
      }

      return (
        <ComposedComponent
          {...this.props}
          data={timeSeries}
          setResolution={this.setResolution}
        />
      )
    },

    /**
     * Graphs can potentially show mulitple kinds of spinners based on whether
     * a graph is being fetched for the first time, or is being refreshed.
     */
    renderFetching(data) {
      const isFirstFetch = !Object.keys(this.state.timeSeries).length
      return (
        <ComposedComponent
          {...this.props}
          data={data}
          setResolution={this.setResolution}
          isFetchingInitially={isFirstFetch}
          isRefreshing={!isFirstFetch}
        />
      )
    },

    renderNoResults() {
      return (
        <div className="graph-empty">
          <p data-test="data-explorer-no-results">No Results</p>
        </div>
      )
    },

    _noResultsForQuery(data) {
      if (!data.length) {
        return true
      }

      return data.every(datum => {
        return datum.response.results.every(result => {
          return (
            Object.keys(result).filter(k => k !== 'statement_id').length === 0
          )
        })
      })
    },
  })

  return wrapper
}

export default AutoRefresh
