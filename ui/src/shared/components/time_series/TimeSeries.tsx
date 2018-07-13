// Library
import React, {Component} from 'react'
import _ from 'lodash'

// API
import {fetchTimeSeries} from 'src/shared/apis/query'

// Types
import {Template, Source, Query, RemoteDataState} from 'src/types'
import {TimeSeriesServerResponse, TimeSeriesResponse} from 'src/types/series'

// Utils
import AutoRefresh from 'src/utils/AutoRefresh'

export const DEFAULT_TIME_SERIES = [{response: {results: []}}]

interface RenderProps {
  timeSeries: TimeSeriesServerResponse[]
  loading: RemoteDataState
}

interface Props {
  inView: boolean
  source: Source
  queries: Query[]
  templates: Template[]
  children: (r: RenderProps) => JSX.Element
}

interface State {
  loading: RemoteDataState
  timeSeries: TimeSeriesServerResponse[]
}

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    inView: true,
  }

  constructor(props: Props) {
    super(props)
    this.state = {
      timeSeries: DEFAULT_TIME_SERIES,
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    this.executeQueries()
    AutoRefresh.subscribe(this.executeQueries)
  }

  public componentWillUnmount() {
    AutoRefresh.unsubscribe(this.executeQueries)
  }

  public async componentDidUpdate(prevProps: Props) {
    if (!this.isPropsDifferent(prevProps)) {
      return
    }

    this.executeQueries()
  }

  public executeQueries = async () => {
    const {source, inView, queries, templates} = this.props

    if (!inView) {
      return
    }

    if (!queries.length) {
      return this.setState({timeSeries: DEFAULT_TIME_SERIES})
    }

    this.setState({loading: RemoteDataState.Loading})

    const TEMP_RES = 300

    try {
      const timeSeries = await fetchTimeSeries(
        source,
        queries,
        TEMP_RES,
        templates
      )

      const newSeries = timeSeries.map((response: TimeSeriesResponse) => ({
        response,
      }))

      this.setState({
        timeSeries: newSeries,
        loading: RemoteDataState.Done,
      })
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    const {timeSeries, loading} = this.state

    const hasValues = _.some(timeSeries, s => {
      const results = _.get(s, 'response.results', [])
      const v = _.some(results, r => r.series)
      return v
    })

    if (!hasValues) {
      return (
        <div className="graph-empty">
          <p>No Results</p>
        </div>
      )
    }

    return this.props.children({timeSeries, loading})
  }

  private isPropsDifferent(nextProps: Props) {
    const isSourceDifferent = !_.isEqual(this.props.source, nextProps.source)

    return (
      this.props.inView !== nextProps.inView ||
      !!this.queryDifference(this.props.queries, nextProps.queries).length ||
      !_.isEqual(this.props.templates, nextProps.templates) ||
      isSourceDifferent
    )
  }

  private queryDifference = (left, right) => {
    const mapper = q => `${q.text}`
    const l = left.map(mapper)
    const r = right.map(mapper)
    return _.difference(_.union(l, r), _.intersection(l, r))
  }
}

export default TimeSeries
