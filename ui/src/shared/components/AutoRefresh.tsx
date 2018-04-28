import React, {Component, ComponentClass} from 'react'
import _ from 'lodash'

import {getQueryConfig} from 'src/shared/apis'
import {fetchTimeSeries} from 'src/shared/apis/query'
import {DEFAULT_TIME_SERIES} from 'src/shared/constants/series'
import {TimeSeriesServerResponse, TimeSeriesResponse} from 'src/types/series'

interface Axes {
  bounds: {
    y: number[]
    y2: number[]
  }
}

interface Query {
  host: string | string[]
  text: string
  database: string
  db: string
  rp: string
}

interface TemplateQuery {
  db: string
  rp: string
  influxql: string
}

interface TemplateValue {
  type: string
  value: string
  selected: boolean
}

interface Template {
  type: string
  tempVar: string
  query: TemplateQuery
  values: TemplateValue[]
}

interface Props {
  type: string
  autoRefresh: number
  inView: boolean
  templates: Template[]
  queries: Query[]
  axes: Axes
  editQueryStatus: () => void
  grabDataForDownload: (timeSeries: TimeSeriesServerResponse[]) => void
}

interface QueryAST {
  groupBy?: {
    tags: string[]
  }
}

interface State {
  isFetching: boolean
  isLastQuerySuccessful: boolean
  timeSeries: TimeSeriesServerResponse[]
  resolution: number | null
  queryASTs?: QueryAST[]
}

interface OriginalProps {
  data: TimeSeriesServerResponse[]
  setResolution: (resolution: number) => void
  isFetchingInitially?: boolean
  isRefreshing?: boolean
  queryASTs?: any[]
}

const AutoRefresh = (
  ComposedComponent: ComponentClass<OriginalProps & Props>
) => {
  class Wrapper extends Component<Props, State> {
    public static defaultProps = {
      inView: true,
    }

    private intervalID: NodeJS.Timer | null

    constructor(props: Props) {
      super(props)
      this.state = {
        isFetching: false,
        isLastQuerySuccessful: true,
        timeSeries: DEFAULT_TIME_SERIES,
        resolution: null,
        queryASTs: [],
      }
    }

    public async componentDidMount() {
      if (this.isTable) {
        const queryASTs = await this.getQueryASTs()
        this.setState({queryASTs})
      }

      this.startNewPolling()
    }

    public async componentDidUpdate(prevProps: Props) {
      if (!this.isPropsDifferent(prevProps)) {
        return
      }

      if (this.isTable) {
        const queryASTs = await this.getQueryASTs()
        this.setState({queryASTs})
      }

      this.startNewPolling()
    }

    public executeQueries = async () => {
      const {editQueryStatus, grabDataForDownload, inView, queries} = this.props
      const {resolution} = this.state

      if (!inView) {
        return
      }

      if (!queries.length) {
        this.setState({timeSeries: DEFAULT_TIME_SERIES})
        return
      }

      this.setState({isFetching: true})
      const templates: Template[] = _.get(this.props, 'templates', [])

      try {
        const timeSeries = await fetchTimeSeries(
          queries,
          resolution,
          templates,
          editQueryStatus
        )
        const newSeries = timeSeries.map((response: TimeSeriesResponse) => ({
          response,
        }))
        const isLastQuerySuccessful = this.hasResultsForQuery(newSeries)

        this.setState({
          timeSeries: newSeries,
          isLastQuerySuccessful,
          isFetching: false,
        })

        if (grabDataForDownload) {
          grabDataForDownload(newSeries)
        }
      } catch (err) {
        console.error(err)
      }
    }

    public componentWillUnmount() {
      this.clearInterval()
    }

    public render() {
      const {
        timeSeries,
        queryASTs,
        isFetching,
        isLastQuerySuccessful,
      } = this.state

      const hasValues = _.some(timeSeries, s => {
        const results = _.get(s, 'response.results', [])
        const v = _.some(results, r => r.series)
        console.error(results, v)
        return v
      })

      if (!hasValues) {
        return (
          <div className="graph-empty">
            <p>No Results</p>
          </div>
        )
      }

      if (isFetching && isLastQuerySuccessful) {
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

    private setResolution = resolution => {
      if (resolution !== this.state.resolution) {
        this.setState({resolution})
      }
    }

    private clearInterval() {
      if (!this.intervalID) {
        return
      }

      clearInterval(this.intervalID)
      this.intervalID = null
    }

    private isPropsDifferent(nextProps: Props) {
      return (
        this.props.inView !== nextProps.inView ||
        !!this.queryDifference(this.props.queries, nextProps.queries).length ||
        !_.isEqual(this.props.templates, nextProps.templates) ||
        this.props.autoRefresh !== nextProps.autoRefresh
      )
    }

    private startNewPolling() {
      this.clearInterval()

      const {autoRefresh} = this.props

      this.executeQueries()

      if (autoRefresh) {
        this.intervalID = setInterval(this.executeQueries, autoRefresh)
      }
    }

    private queryDifference = (left, right) => {
      const mapper = q => `${q.host}${q.text}`
      const leftStrs = left.map(mapper)
      const rightStrs = right.map(mapper)
      return _.difference(
        _.union(leftStrs, rightStrs),
        _.intersection(leftStrs, rightStrs)
      )
    }

    private get isTable(): boolean {
      return this.props.type === 'table'
    }

    private getQueryASTs = async (): Promise<QueryAST[]> => {
      const {queries, templates} = this.props

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

    private hasResultsForQuery = (data): boolean => {
      if (!data.length) {
        return false
      }

      data.every(({resp}) =>
        _.get(resp, 'results', []).every(r => Object.keys(r).length > 1)
      )
    }
  }

  return Wrapper
}

export default AutoRefresh
