// Library
import React, {Component} from 'react'
import _ from 'lodash'

// API
import {fetchTimeSeries} from 'src/shared/apis/v2/timeSeries'

// Types
import {Template, CellQuery, RemoteDataState} from 'src/types'
import {TimeSeriesServerResponse, TimeSeriesResponse} from 'src/types/series'

// Utils
import AutoRefresh from 'src/utils/AutoRefresh'

export const DEFAULT_TIME_SERIES = [{response: {results: []}}]

interface RenderProps {
  timeSeries: TimeSeriesServerResponse[]
  loading: RemoteDataState
}

interface Props {
  link: string
  queries: CellQuery[]
  children: (r: RenderProps) => JSX.Element
  inView?: boolean
  templates?: Template[]
}

interface State {
  loading: RemoteDataState
  isFirstFetch: boolean
  timeSeries: TimeSeriesServerResponse[]
}

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    inView: true,
    templates: [],
  }

  constructor(props: Props) {
    super(props)
    this.state = {
      timeSeries: DEFAULT_TIME_SERIES,
      loading: RemoteDataState.NotStarted,
      isFirstFetch: false,
    }
  }

  public async componentDidMount() {
    const isFirstFetch = true
    this.executeQueries(isFirstFetch)
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

  public executeQueries = async (isFirstFetch: boolean = false) => {
    const {link, inView, queries, templates} = this.props

    if (!inView) {
      return
    }

    if (!queries.length) {
      return this.setState({timeSeries: DEFAULT_TIME_SERIES})
    }

    this.setState({loading: RemoteDataState.Loading, isFirstFetch})

    const TEMP_RES = 300

    try {
      const timeSeries = await fetchTimeSeries(
        link,
        this.queries,
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
    const {timeSeries, loading, isFirstFetch} = this.state

    const hasValues = _.some(timeSeries, s => {
      const results = _.get(s, 'response.results', [])
      const v = _.some(results, r => r.series)
      return v
    })

    if (isFirstFetch && loading === RemoteDataState.Loading) {
      return (
        <div className="graph-empty">
          <h3 className="graph-spinner" />
        </div>
      )
    }

    if (!hasValues) {
      return (
        <div className="graph-empty">
          <p>No Results</p>
        </div>
      )
    }

    return this.props.children({timeSeries, loading})
  }

  private get queries(): string[] {
    return this.props.queries.map(q => q.text)
  }

  private isPropsDifferent(nextProps: Props) {
    const isSourceDifferent = !_.isEqual(this.props.link, nextProps.link)

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
