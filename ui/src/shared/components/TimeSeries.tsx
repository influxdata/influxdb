// Library
import React, {Component, RefObject, CSSProperties} from 'react'
import {isEqual} from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {fromFlux, FromFluxResult} from '@influxdata/giraffe'

// API
import {
  runQuery,
  RunQueryResult,
  RunQuerySuccessResult,
} from 'src/shared/apis/query'
import {runStatusesQuery} from 'src/alerting/utils/statusEvents'

// Utils
import {checkQueryResult} from 'src/shared/utils/checkQueryResult'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import 'intersection-observer'
import {getAll} from 'src/resources/selectors'
import {getOrgIDFromBuckets} from 'src/timeMachine/actions/queries'

// Constants
import {rateLimitReached, resultTooLarge} from 'src/shared/copy/notifications'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {
  RemoteDataState,
  Check,
  StatusRow,
  Bucket,
  ResourceType,
  DashboardQuery,
  VariableAssignment,
  AppState,
  CancelBox,
} from 'src/types'

interface QueriesState {
  files: string[] | null
  loading: RemoteDataState
  errorMessage: string
  isInitialFetch: boolean
  duration: number
  giraffeResult: FromFluxResult
  statuses: StatusRow[][]
}

interface StateProps {
  queryLink: string
  buckets: Bucket[]
}

interface OwnProps {
  className: string
  style: CSSProperties
  queries: DashboardQuery[]
  variables?: VariableAssignment[]
  submitToken: number
  implicitSubmit?: boolean
  children: (r: QueriesState) => JSX.Element
  check: Partial<Check>
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = StateProps & OwnProps & DispatchProps

interface State {
  loading: RemoteDataState
  files: string[] | null
  errorMessage: string
  fetchCount: number
  duration: number
  giraffeResult: FromFluxResult
  statuses: StatusRow[][]
}

const defaultState = (): State => ({
  loading: RemoteDataState.NotStarted,
  files: null,
  fetchCount: 0,
  errorMessage: '',
  duration: 0,
  giraffeResult: null,
  statuses: [[]],
})

class TimeSeries extends Component<Props & WithRouterProps, State> {
  public static defaultProps = {
    implicitSubmit: true,
    className: 'time-series-container',
    style: null,
  }

  public state: State = defaultState()

  private observer: IntersectionObserver
  private ref: RefObject<HTMLDivElement> = React.createRef()
  private isIntersecting: boolean = false
  private pendingReload: boolean = true

  private pendingResults: Array<CancelBox<RunQueryResult>> = []
  private pendingCheckStatuses: CancelBox<StatusRow[][]> = null

  public componentDidMount() {
    this.observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        const {isIntersecting} = entry
        if (!this.isIntersecting && isIntersecting && this.pendingReload) {
          this.reload()
        }

        this.isIntersecting = isIntersecting
      })
    })

    this.observer.observe(this.ref.current)
  }

  public componentDidUpdate(prevProps: Props) {
    if (this.shouldReload(prevProps) && this.isIntersecting) {
      this.reload()
    }
  }

  public componentWillUnmount() {
    this.observer && this.observer.disconnect()
  }

  public render() {
    const {
      giraffeResult,
      files,
      loading,
      errorMessage,
      fetchCount,
      duration,
      statuses,
    } = this.state
    const {className, style} = this.props

    return (
      <div ref={this.ref} className={className} style={style}>
        {this.props.children({
          giraffeResult,
          files,
          loading,
          errorMessage,
          duration,
          isInitialFetch: fetchCount === 1,
          statuses,
        })}
      </div>
    )
  }

  private reload = async () => {
    const {variables, notify, check, buckets} = this.props
    const queries = this.props.queries.filter(({text}) => !!text.trim())

    if (!queries.length) {
      this.setState(defaultState())

      return
    }

    this.setState({
      loading: RemoteDataState.Loading,
      fetchCount: this.state.fetchCount + 1,
      errorMessage: '',
    })

    try {
      const startTime = Date.now()

      // Cancel any existing queries
      this.pendingResults.forEach(({cancel}) => cancel())

      // Issue new queries
      this.pendingResults = queries.map(({text}) => {
        const orgID =
          getOrgIDFromBuckets(text, buckets) || this.props.params.orgID

        const windowVars = getWindowVars(text, variables)
        const extern = buildVarsOption([...variables, ...windowVars])

        return runQuery(orgID, text, extern)
      })

      // Wait for new queries to complete
      const results = await Promise.all(this.pendingResults.map(r => r.promise))

      let statuses = [] as StatusRow[][]
      if (check) {
        const extern = buildVarsOption(variables)
        this.pendingCheckStatuses = runStatusesQuery(
          this.props.params.orgID,
          check.id,
          extern
        )
        statuses = await this.pendingCheckStatuses.promise // TODO handle errors
      }

      const duration = Date.now() - startTime

      for (const result of results) {
        if (result.type === 'UNKNOWN_ERROR') {
          throw new Error(result.message)
        }

        if (result.type === 'RATE_LIMIT_ERROR') {
          notify(rateLimitReached(result.retryAfter))

          throw new Error(result.message)
        }

        if (result.didTruncate) {
          notify(resultTooLarge(result.bytesRead))
        }

        checkQueryResult(result.csv)
      }

      const files = (results as RunQuerySuccessResult[]).map(r => r.csv)
      const giraffeResult = fromFlux(files.join('\n\n'))
      this.pendingReload = false

      this.setState({
        giraffeResult,
        files,
        duration,
        loading: RemoteDataState.Done,
        statuses,
      })
    } catch (error) {
      if (error.name === 'CancellationError') {
        return
      }

      console.error(error)

      this.setState({
        errorMessage: error.message,
        giraffeResult: null,
        loading: RemoteDataState.Error,
        statuses: [[]],
      })
    }

    this.pendingReload = false
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

const mstp = (state: AppState): StateProps => {
  const {links} = state

  return {
    queryLink: links.query.self,
    buckets: getAll<Bucket>(state, ResourceType.Buckets),
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  mdtp
)(withRouter(TimeSeries))
