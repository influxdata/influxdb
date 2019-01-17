// Library
import {Component} from 'react'
import {isEqual, flatten} from 'lodash'
import {connect} from 'react-redux'

// API
import {executeQuery, ExecuteFluxQueryResult} from 'src/shared/apis/v2/query'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'
import {getSources, getActiveSource} from 'src/sources/selectors'
import {renderQuery} from 'src/shared/utils/renderQuery'

// Types
import {RemoteDataState, FluxTable} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'
import {AppState, Source} from 'src/types/v2'
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'

type URLQuery = DashboardQuery & {url: string}

const executeRenderedQuery = (
  {text, type, url}: URLQuery,
  variables: {[key: string]: string}
): WrappedCancelablePromise<ExecuteFluxQueryResult> => {
  let isCancelled = false
  let cancelExecution

  const cancel = () => {
    isCancelled = true

    if (cancelExecution) {
      cancelExecution()
    }
  }

  const promise = renderQuery(text, type, variables).then(renderedQuery => {
    if (isCancelled) {
      return Promise.reject(new CancellationError())
    }

    const pendingResult = executeQuery(url, renderedQuery, type)

    cancelExecution = pendingResult.cancel

    return pendingResult.promise
  })

  return {promise, cancel}
}

export interface QueriesState {
  tables: FluxTable[]
  files: string[] | null
  loading: RemoteDataState
  error: Error | null
  isInitialFetch: boolean
  duration: number
}

interface StateProps {
  dynamicSourceURL: string
  sources: Source[]
}

interface OwnProps {
  queries: DashboardQuery[]
  variables?: {[key: string]: string}
  submitToken: number
  implicitSubmit?: boolean
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
  duration: number
}

const defaultState = (): State => ({
  loading: RemoteDataState.NotStarted,
  tables: [],
  files: null,
  fetchCount: 0,
  error: null,
  duration: 0,
})

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    inView: true,
    implicitSubmit: true,
  }

  public state: State = defaultState()

  private pendingResults: Array<
    WrappedCancelablePromise<ExecuteFluxQueryResult>
  > = []

  public async componentDidMount() {
    this.reload()
  }

  public async componentDidUpdate(prevProps: Props) {
    if (this.shouldReload(prevProps)) {
      this.reload()
    }
  }

  public render() {
    const {tables, files, loading, error, fetchCount, duration} = this.state

    return this.props.children({
      tables,
      files,
      loading,
      error,
      duration,
      isInitialFetch: fetchCount === 1,
    })
  }

  private get queries(): URLQuery[] {
    const {sources, queries, dynamicSourceURL} = this.props

    return queries.filter(query => !!query.text).map(query => {
      const source = sources.find(source => source.id === query.sourceID)
      const url: string = source ? source.links.query : dynamicSourceURL

      return {...query, url}
    })
  }

  private reload = async () => {
    const {inView, variables} = this.props
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
      error: null,
    })

    try {
      const startTime = Date.now()

      // Cancel any existing queries
      this.pendingResults.forEach(({cancel}) => cancel())

      // Issue new queries
      this.pendingResults = queries.map(q => executeRenderedQuery(q, variables))

      // Wait for new queries to complete
      const results = await Promise.all(this.pendingResults.map(r => r.promise))

      const duration = Date.now() - startTime
      const tables = flatten(results.map(r => parseResponse(r.csv)))
      const files = results.map(r => r.csv)

      this.setState({
        tables,
        files,
        duration,
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

    if (!this.props.implicitSubmit) {
      return false
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
