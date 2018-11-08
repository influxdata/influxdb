// Library
import {Component} from 'react'
import {isEqual, flatten} from 'lodash'

// API
import {executeQueries} from 'src/shared/apis/v2/query'

// Types
import {RemoteDataState, FluxTable} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {parseResponse} from 'src/shared/parsing/flux/response'
import {restartable, CancellationError} from 'src/utils/restartable'

export const DEFAULT_TIME_SERIES = [{response: {results: []}}]

interface RenderProps {
  tables: FluxTable[]
  loading: RemoteDataState
  error: Error | null
  isInitialFetch: boolean
}

interface Props {
  link: string
  queries: DashboardQuery[]
  inView?: boolean
  children: (r: RenderProps) => JSX.Element
}

interface State {
  loading: RemoteDataState
  tables: FluxTable[]
  error: Error | null
  fetchCount: number
}

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    inView: true,
  }

  private executeQueries = restartable(executeQueries)

  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
      tables: [],
      fetchCount: 0,
      error: null,
    }
  }

  public async componentDidMount() {
    this.reload()

    GlobalAutoRefresher.subscribe(this.reload)
  }

  public componentWillUnmount() {
    GlobalAutoRefresher.unsubscribe(this.reload)
  }

  public async componentDidUpdate(prevProps: Props) {
    if (this.shouldReload(prevProps)) {
      this.reload()
    }
  }

  public render() {
    const {tables, loading, error, fetchCount} = this.state

    return this.props.children({
      tables,
      loading,
      error,
      isInitialFetch: fetchCount === 1,
    })
  }

  private reload = async () => {
    const {link, inView, queries} = this.props

    if (!inView) {
      return
    }

    this.setState({
      loading: RemoteDataState.Loading,
      fetchCount: this.state.fetchCount + 1,
    })

    try {
      const results = await this.executeQueries(link, queries)
      const tables = flatten(results.map(r => parseResponse(r.csv)))

      this.setState({
        tables,
        loading: RemoteDataState.Done,
      })
    } catch (error) {
      if (error instanceof CancellationError) {
        return
      }

      this.setState({
        error,
        loading: RemoteDataState.Error,
      })
    }
  }

  private shouldReload(prevProps: Props) {
    if (prevProps.link !== this.props.link) {
      return true
    }

    if (!isEqual(prevProps.queries, this.props.queries)) {
      return true
    }

    return false
  }
}

export default TimeSeries
