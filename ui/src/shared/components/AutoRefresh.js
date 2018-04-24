import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'
import {removeUnselectedTemplateValues} from 'src/dashboards/constants'
import {intervalValuesPoints} from 'src/shared/constants'
import {getQueryConfig} from 'shared/apis'

const AutoRefresh = ComposedComponent => {
  class wrapper extends Component {
    constructor() {
      super()
      this.state = {
        lastQuerySuccessful: true,
        timeSeries: [],
        resolution: null,
        queryASTs: [],
      }
    }

    async componentDidMount() {
      const {queries, templates, autoRefresh, type} = this.props
      this.executeQueries(queries, templates)
      if (type === 'table') {
        const queryASTs = await this.getQueryASTs(queries, templates)
        this.setState({queryASTs})
      }
      if (autoRefresh) {
        this.intervalID = setInterval(
          () => this.executeQueries(queries, templates),
          autoRefresh
        )
      }
    }

    getQueryASTs = async (queries, templates) => {
      return await Promise.all(
        queries.map(async q => {
          const host = _.isArray(q.host) ? q.host[0] : q.host
          const url = host.replace('proxy', 'queries')
          const text = q.text
          const {data} = await getQueryConfig(url, [{query: text}], templates)
          return data.queries[0].queryAST
        })
      )
    }

    async componentWillReceiveProps(nextProps) {
      const inViewDidUpdate = this.props.inView !== nextProps.inView

      const queriesDidUpdate = this.queryDifference(
        this.props.queries,
        nextProps.queries
      ).length

      const tempVarsDidUpdate = !_.isEqual(
        this.props.templates,
        nextProps.templates
      )

      const shouldRefetch =
        queriesDidUpdate || tempVarsDidUpdate || inViewDidUpdate

      if (shouldRefetch) {
        if (this.props.type === 'table') {
          const queryASTs = await this.getQueryASTs(
            nextProps.queries,
            nextProps.templates
          )
          this.setState({queryASTs})
        }

        this.executeQueries(
          nextProps.queries,
          nextProps.templates,
          nextProps.inView
        )
      }

      if (this.props.autoRefresh !== nextProps.autoRefresh || shouldRefetch) {
        clearInterval(this.intervalID)

        if (nextProps.autoRefresh) {
          this.intervalID = setInterval(
            () =>
              this.executeQueries(
                nextProps.queries,
                nextProps.templates,
                nextProps.inView
              ),
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

    executeQueries = async (
      queries,
      templates = [],
      inView = this.props.inView
    ) => {
      const {editQueryStatus, grabDataForDownload} = this.props
      const {resolution} = this.state
      if (!inView) {
        return
      }
      if (!queries.length) {
        this.setState({timeSeries: []})
        return
      }

      this.setState({isFetching: true})

      const timeSeriesPromises = queries.map(query => {
        const {host, database, rp} = query
        // the key `database` was used upstream in HostPage.js, and since as of this writing
        // the codebase has not been fully converted to TypeScript, it's not clear where else
        // it may be used, but this slight modification is intended to allow for the use of
        // `database` while moving over to `db` for consistency over time
        const db = _.get(query, 'db', database)

        const templatesWithIntervalVals = templates.map(temp => {
          if (temp.tempVar === ':interval:') {
            if (resolution) {
              // resize event
              return {
                ...temp,
                values: temp.values.map(v => ({
                  ...v,
                  value: `${_.toInteger(Number(resolution) / 3)}`,
                })),
              }
            }

            return {
              ...temp,
              values: intervalValuesPoints,
            }
          }
          return temp
        })

        const tempVars = removeUnselectedTemplateValues(
          templatesWithIntervalVals
        )
        return fetchTimeSeriesAsync(
          {
            source: host,
            db,
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
      if (resolution !== this.state.resolution) {
        this.setState({resolution})
      }
    }

    render() {
      const {timeSeries, queryASTs} = this.state
      if (this.state.isFetching && this.state.lastQuerySuccessful) {
        return (
          <ComposedComponent
            {...this.props}
            data={timeSeries}
            setResolution={this.setResolution}
            isFetchingInitially={false}
            isRefreshing={true}
            queryASTs={queryASTs}
          />
        )
      }

      return (
        <ComposedComponent
          {...this.props}
          data={timeSeries}
          setResolution={this.setResolution}
          queryASTs={queryASTs}
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

  wrapper.defaultProps = {
    inView: true,
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
    type: string.isRequired,
    children: element,
    autoRefresh: number.isRequired,
    inView: bool,
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
