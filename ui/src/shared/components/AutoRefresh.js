import React, {PropTypes, Component} from 'react'
import _ from 'lodash'

import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'
import {removeUnselectedTemplateValues} from 'src/dashboards/constants'

const AutoRefresh = ComposedComponent => {
  class wrapper extends Component {
    constructor() {
      super()
      this.state = {
        lastQuerySuccessful: false,
        timeSeries: [],
        resolution: null,
      }
    }

    componentDidMount() {
      const {queries, templates, autoRefresh} = this.props
      this.executeQueries(queries, templates)
      if (autoRefresh) {
        this.intervalID = setInterval(
          () => this.executeQueries(queries, templates),
          autoRefresh
        )
      }
    }

    componentWillReceiveProps(nextProps) {
      const preventLoadDidUpdate =
        this.props.preventLoad && !nextProps.preventLoad

      const queriesDidUpdate = this.queryDifference(
        this.props.queries,
        nextProps.queries
      ).length

      const tempVarsDidUpdate = !_.isEqual(
        this.props.templates,
        nextProps.templates
      )

      const shouldRefetch =
        queriesDidUpdate || tempVarsDidUpdate || preventLoadDidUpdate

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
    }

    queryDifference = (left, right) => {
      const leftStrs = left.map(q => `${q.host}${q.text}`)
      const rightStrs = right.map(q => `${q.host}${q.text}`)
      return _.difference(
        _.union(leftStrs, rightStrs),
        _.intersection(leftStrs, rightStrs)
      )
    }

    executeQueries = async (queries, templates = []) => {
      const {editQueryStatus, grabDataForDownload, preventLoad} = this.props
      const {resolution} = this.state
      if (preventLoad) {
        this.setState({timeSeries: []})
        return
      }
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
              return {
                ...temp,
                values: temp.values.map(
                  v => (temp.type === 'resolution' ? {...v, resolution} : v)
                ),
              }
            }

            return {
              ...temp,
              values: [
                ...temp.values,
                {value: '1000', type: 'resolution', selected: true},
              ],
            }
          }

          return temp
        })

        const tempVars = removeUnselectedTemplateValues(templatesWithResolution)

        return fetchTimeSeriesAsync(
          {
            source: host,
            db: database,
            rp,
            query,
            tempVars,
            resolution,
          },
          editQueryStatus
        )
      })

      try {
        const timeSeries = await Promise.all(timeSeriesPromises)
        const newSeries = timeSeries.map(response => ({response}))
        const lastQuerySuccessful = this._resultsForQuery(newSeries)

        this.setState({
          timeSeries: newSeries,
          lastQuerySuccessful,
          isFetching: false,
        })

        if (grabDataForDownload) {
          grabDataForDownload(timeSeries)
        }
      } catch (err) {
        console.error(err)
      }
    }

    componentWillUnmount() {
      clearInterval(this.intervalID)
      this.intervalID = false
    }

    setResolution = resolution => {
      this.setState({resolution})
    }

    render() {
      const {timeSeries} = this.state

      if (this.state.isFetching && this.state.lastQuerySuccessful) {
        return this.renderFetching(timeSeries)
      }

      return (
        <ComposedComponent
          {...this.props}
          data={timeSeries}
          setResolution={this.setResolution}
        />
      )
    }

    /**
     * Graphs can potentially show mulitple kinds of spinners based on whether
     * a graph is being fetched for the first time, or is being refreshed.
     */
    renderFetching = data => {
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
    }

    _resultsForQuery = data =>
      data.length
        ? data.every(({response}) =>
            _.get(response, 'results', []).every(
              result =>
                Object.keys(result).filter(k => k !== 'statement_id').length !==
                0
            )
          )
        : false
  }

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

  wrapper.propTypes = {
    children: element,
    autoRefresh: number.isRequired,
    preventLoad: bool,
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
    grabDataForDownload: func,
  }

  return wrapper
}

export default AutoRefresh
