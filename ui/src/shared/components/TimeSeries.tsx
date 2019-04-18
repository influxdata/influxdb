// Library
import {Component} from 'react'
import {isEqual, flatten, get} from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// API
import {
  executeQueryWithVars,
  ExecuteFluxQueryResult,
} from 'src/shared/apis/query'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'
import {checkQueryResult} from 'src/shared/utils/checkQueryResult'

// Constants
import {RATE_LIMIT_ERROR_STATUS} from 'src/shared/constants/errors'
import {queryLimitReached} from 'src/shared/copy/notifications'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {RemoteDataState, FluxTable} from 'src/types'
import {DashboardQuery} from 'src/types/dashboards'
import {AppState} from 'src/types'
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {VariableAssignment} from 'src/types/ast'

interface QueriesState {
  tables: FluxTable[]
  files: string[] | null
  loading: RemoteDataState
  error: Error | null
  isInitialFetch: boolean
  duration: number
}

interface StateProps {
  queryLink: string
}

interface OwnProps {
  queries: DashboardQuery[]
  variables?: VariableAssignment[]
  submitToken: number
  implicitSubmit?: boolean
  inView?: boolean
  children: (r: QueriesState) => JSX.Element
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = StateProps & OwnProps & DispatchProps

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

class TimeSeries extends Component<Props & WithRouterProps, State> {
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

  private reload = async () => {
    const {inView, queryLink, variables, notify} = this.props
    const queries = this.props.queries.filter(({text}) => !!text.trim())
    const orgID = this.props.params.orgID

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
      this.pendingResults = queries.map(({text}) =>
        executeQueryWithVars(queryLink, orgID, text, variables)
      )

      // Wait for new queries to complete
      const results = await Promise.all(this.pendingResults.map(r => r.promise))

      const duration = Date.now() - startTime
      const tables = flatten(results.map(r => parseResponse(r.csv)))
      const files = results.map(r => r.csv)

      files.forEach(checkQueryResult)

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

      if (get(error, 'xhr.status') === RATE_LIMIT_ERROR_STATUS) {
        notify(queryLimitReached())
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

    if (!isEqual(prevProps.variables, this.props.variables)) {
      return true
    }

    return false
  }
}

const mstp = (state: AppState) => {
  const {links} = state

  return {queryLink: links.query.self}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  mdtp
)(withRouter(TimeSeries))
