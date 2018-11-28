// Library
import {Component} from 'react'
import {isEqual, flatten} from 'lodash'
import {connect} from 'react-redux'

// API
import {executeQueries} from 'src/shared/apis/v2/query'

// Types
import {RemoteDataState, FluxTable} from 'src/types'
import {DashboardQuery, URLQuery} from 'src/types/v2/dashboards'
import {AppState, Source} from 'src/types/v2'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'
import {restartable, CancellationError} from 'src/utils/restartable'
import {getSources, getActiveSource} from 'src/sources/selectors'

export const DEFAULT_TIME_SERIES = [{response: {results: []}}]

export interface QueriesState {
  tables: FluxTable[]
  files: string[] | null
  loading: RemoteDataState
  error: Error | null
  isInitialFetch: boolean
}

interface StateProps {
  dynamicSourceURL: string
  sources: Source[]
}

interface OwnProps {
  queries: DashboardQuery[]
  submitToken: number
  inView?: boolean
  children: (r: QueriesState) => JSX.Element
}

type Props = StateProps & OwnProps

interface State {
  loading: RemoteDataState
  tables: FluxTable[]
  files: string[] | null
  error: Error | null
  fetchCount: number
}

const defaultState = (): State => ({
  loading: RemoteDataState.NotStarted,
  tables: [],
  files: null,
  fetchCount: 0,
  error: null,
})

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    inView: true,
  }

  public state: State = defaultState()

  private executeQueries = restartable(executeQueries)

  public async componentDidMount() {
    this.reload()
  }

  public async componentDidUpdate(prevProps: Props) {
    if (this.shouldReload(prevProps)) {
      this.reload()
    }
  }

  public render() {
    const {tables, files, loading, error, fetchCount} = this.state

    return this.props.children({
      tables,
      files,
      loading,
      error,
      isInitialFetch: fetchCount === 1,
    })
  }

  private get queries(): URLQuery[] {
    const {sources, queries, dynamicSourceURL} = this.props

    return queries.filter(query => !!query.text).map(query => {
      const source = sources.find(source => source.id === query.sourceID)
      const url: string = source ? source.links.query : dynamicSourceURL

      return {url, text: query.text, type: query.type}
    })
  }

  private reload = async () => {
    const {inView} = this.props
    const queries = this.queries

    if (!inView) {
      return
    }

    if (!queries.length) {
      this.setState(defaultState())

      return
    }

    this.setState({
      loading: RemoteDataState.Loading,
      fetchCount: this.state.fetchCount + 1,
    })

    try {
      const results = await this.executeQueries(queries)
      const tables = flatten(results.map(r => parseResponse(r.csv)))
      const files = results.map(r => r.csv)

      this.setState({
        tables,
        files,
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
    if (prevProps.submitToken !== this.props.submitToken) {
      return true
    }

    if (!isEqual(prevProps.queries, this.props.queries)) {
      return true
    }

    if (prevProps.dynamicSourceURL !== this.props.dynamicSourceURL) {
      return true
    }

    return false
  }
}

const mstp = (state: AppState) => {
  const sources = getSources(state)
  const dynamicSourceURL = getActiveSource(state).links.query

  return {sources, dynamicSourceURL}
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(TimeSeries)
