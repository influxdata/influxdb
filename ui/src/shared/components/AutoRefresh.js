import React, {PropTypes} from 'react'
import _ from 'lodash'
import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'

const {
  arrayOf,
  element,
  func,
  number,
  oneOfType,
  shape,
  string,
} = PropTypes

const AutoRefresh = (ComposedComponent) => {
  const wrapper = React.createClass({
    propTypes: {
      children: element,
      autoRefresh: number.isRequired,
      queries: arrayOf(shape({
        host: oneOfType([string, arrayOf(string)]),
        text: string,
      }).isRequired).isRequired,
      editQueryStatus: func,
    },
    getInitialState() {
      return {
        lastQuerySuccessful: false,
        timeSeries: [],
      }
    },
    componentDidMount() {
      const {queries, autoRefresh} = this.props
      this.executeQueries(queries)
      if (autoRefresh) {
        this.intervalID = setInterval(() => this.executeQueries(queries), autoRefresh)
      }
    },
    componentWillReceiveProps(nextProps) {
      const shouldRefetch = this.queryDifference(this.props.queries, nextProps.queries).length

      if (shouldRefetch) {
        this.executeQueries(nextProps.queries)
      }

      if ((this.props.autoRefresh !== nextProps.autoRefresh) || shouldRefetch) {
        clearInterval(this.intervalID)

        if (nextProps.autoRefresh) {
          this.intervalID = setInterval(() => this.executeQueries(nextProps.queries), nextProps.autoRefresh)
        }
      }
    },
    queryDifference(left, right) {
      const leftStrs = left.map((q) => `${q.host}${q.text}`)
      const rightStrs = right.map((q) => `${q.host}${q.text}`)
      return _.difference(_.union(leftStrs, rightStrs), _.intersection(leftStrs, rightStrs))
    },
    async executeQueries(queries) {
      if (!queries.length) {
        this.setState({
          timeSeries: [],
        })
        return
      }

      this.setState({isFetching: true})
      let count = 0
      const newSeries = []
      for (const query of queries) {
        const {host, database, rp} = query
        // TODO: enact this via an action creator so redux will know about it; currently errors are used as responses here
        // TODO: may need to make this a try/catch
        const response = await fetchTimeSeriesAsync({source: host, db: database, rp, query}, this.props.editQueryStatus)
        newSeries.push({response})
        count += 1
        if (count === queries.length) {
          const querySuccessful = !this._noResultsForQuery(newSeries)
          this.setState({
            lastQuerySuccessful: querySuccessful,
            isFetching: false,
            timeSeries: newSeries,
          })
        }
      }
    },
    componentWillUnmount() {
      clearInterval(this.intervalID)
      this.intervalID = false
    },
    render() {
      const {timeSeries} = this.state

      if (this.state.isFetching && this.state.lastQuerySuccessful) {
        return this.renderFetching(timeSeries)
      }

      if (this._noResultsForQuery(timeSeries) || !this.state.lastQuerySuccessful) {
        return this.renderNoResults()
      }

      return (
        <ComposedComponent
          {...this.props}
          data={timeSeries}
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
          isFetchingInitially={isFirstFetch}
          isRefreshing={!isFirstFetch}
        />
      )
    },

    renderNoResults() {
      if (this.props.children) {
        return this.props.children
      }

      return (
        <div className="graph-empty">
          <p>No Results</p>
        </div>
      )
    },

    _noResultsForQuery(data) {
      if (!data.length) {
        return true
      }

      return data.every((datum) => {
        return datum.response.results.every((result) => {
          return Object.keys(result).length === 0
        })
      })
    },
  })

  return wrapper
}

export default AutoRefresh
