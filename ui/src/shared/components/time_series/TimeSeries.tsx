// Library
import React, {Component} from 'react'
import _ from 'lodash'

// API
import {fetchTimeSeries} from 'src/shared/apis/v2/timeSeries'

// Types
import {Template, RemoteDataState, FluxTable} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'

// Utils
import AutoRefresh from 'src/utils/AutoRefresh'

export const DEFAULT_TIME_SERIES = [{response: {results: []}}]

interface RenderProps {
  tables: FluxTable[]
  loading: RemoteDataState
}

interface Props {
  link: string
  queries: DashboardQuery[]
  inView?: boolean
  templates?: Template[]
  children: (r: RenderProps) => JSX.Element
}

interface State {
  loading: RemoteDataState
  isFirstFetch: boolean
  tables: FluxTable[]
}

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    inView: true,
    templates: [],
  }

  constructor(props: Props) {
    super(props)
    this.state = {
      loading: RemoteDataState.NotStarted,
      isFirstFetch: false,
      tables: [],
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
      return this.setState({tables: []})
    }

    this.setState({loading: RemoteDataState.Loading, isFirstFetch})

    const TEMP_RES = 300

    try {
      const tables = await fetchTimeSeries(link, queries, TEMP_RES, templates)

      this.setState({
        tables,
        loading: RemoteDataState.Done,
      })
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    const {tables, loading, isFirstFetch} = this.state

    const hasValues = _.some(tables, s => {
      const data = _.get(s, 'data', [])
      return !!data.length
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

    return this.props.children({tables, loading})
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
