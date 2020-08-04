// Library
import React, {Component, RefObject, CSSProperties} from 'react'
import {isEqual} from 'lodash'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {
  default as fromFlux,
  FromFluxResult,
} from 'src/shared/utils/fromFlux.legacy'
import {fromFlux as fromFluxGiraffe} from '@influxdata/giraffe'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// API
import {RunQueryResult, RunQuerySuccessResult} from 'src/shared/apis/query'
import {runStatusesQuery} from 'src/alerting/utils/statusEvents'
import {getCachedResultsThunk} from 'src/shared/apis/queryCache'

// Utils
import {
  getTimeRange,
  isCurrentPageDashboard as isCurrentPageDashboardSelector,
} from 'src/dashboards/selectors'
import {getVariables, asAssignment} from 'src/variables/selectors'
import {getRangeVariable} from 'src/variables/utils/getTimeRangeVars'
import {isInQuery} from 'src/variables/utils/hydrateVars'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import 'intersection-observer'
import {getAll} from 'src/resources/selectors'
import {getOrgIDFromBuckets} from 'src/timeMachine/actions/queries'
import {
  isDemoDataAvailabilityError,
  demoDataError,
} from 'src/cloud/utils/demoDataErrors'
import {hashCode} from 'src/shared/apis/queryCache'
import {RunQueryPromiseMutex} from 'src/shared/apis/singleQuery'

// Constants
import {
  rateLimitReached,
  resultTooLarge,
  demoDataAvailability,
} from 'src/shared/copy/notifications'
import {TIME_RANGE_START, TIME_RANGE_STOP} from 'src/variables/constants'

// Actions & Selectors
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {hasUpdatedTimeRangeInVEO} from 'src/shared/selectors/app'
import {setCellMount as setCellMountAction} from 'src/perf/actions'

// Types
import {
  RemoteDataState,
  Check,
  StatusRow,
  Bucket,
  ResourceType,
  DashboardQuery,
  AppState,
  CancelBox,
} from 'src/types'
import {event} from 'src/cloud/utils/reporting'

interface QueriesState {
  files: string[] | null
  loading: RemoteDataState
  errorMessage: string
  isInitialFetch: boolean
  duration: number
  giraffeResult: FromFluxResult
  statuses: StatusRow[][]
}

interface OwnProps {
  cellID?: string
  className?: string
  style?: CSSProperties
  queries: DashboardQuery[]
  submitToken: number
  implicitSubmit?: boolean
  children: (r: QueriesState) => JSX.Element
  check?: Partial<Check>
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps & RouteComponentProps<{orgID: string}>

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

type HashMapMutex = {
  [queryID: string]: ReturnType<typeof RunQueryPromiseMutex>
}

class TimeSeries extends Component<Props, State> {
  public static defaultProps = {
    implicitSubmit: true,
    className: 'time-series-container',
    style: null,
  }
  public state: State = defaultState()

  private hashMapMutex: HashMapMutex = {}

  private observer: IntersectionObserver
  private ref: RefObject<HTMLDivElement> = React.createRef()
  private isIntersecting: boolean = false
  private pendingReload: boolean = true

  private pendingResults: Array<CancelBox<RunQueryResult>> = []
  private pendingCheckStatuses: CancelBox<StatusRow[][]> = null

  public componentDidMount() {
    const {cellID, setCellMount} = this.props
    this.observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        const {isIntersecting} = entry
        const reload =
          !this.isIntersecting && isIntersecting && this.pendingReload

        if (reload) {
          this.reload()
        }

        if (reload && cellID) {
          setCellMount(cellID, new Date().getTime())
        }

        this.isIntersecting = isIntersecting
      })
    })

    this.observer.observe(this.ref.current)
  }

  public componentDidUpdate(prevProps: Props) {
    const {setCellMount, cellID} = this.props
    const reload = this.shouldReload(prevProps) && this.isIntersecting
    if (reload) {
      this.reload()
    }

    if (reload && cellID) {
      setCellMount(cellID, new Date().getTime())
    }
  }

  public componentWillUnmount() {
    this.observer && this.observer.disconnect()
    this.pendingResults.forEach(({cancel}) => cancel())
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
    const {
      buckets,
      check,
      isCurrentPageDashboard,
      notify,
      onGetCachedResultsThunk,
      variables,
    } = this.props
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
      let errorMessage: string = ''

      // Cancel any existing queries
      this.pendingResults.forEach(({cancel}) => {
        if (cancel) {
          cancel()
        }
      })
      const usedVars = variables.filter(v => v.arguments.type !== 'system')
      const waitList = usedVars.filter(v => v.status !== RemoteDataState.Done)

      // If a variable is loading, and a cell requires it, leave the cell to never resolve,
      // keeping it in a loading state until the variable is resolved
      if (usedVars.length && waitList.length) {
        await new Promise(() => {})
      }

      const vars = variables.map(v => asAssignment(v))
      // Issue new queries
      this.pendingResults = queries.map(({text}) => {
        const orgID =
          getOrgIDFromBuckets(text, buckets) || this.props.match.params.orgID

        const windowVars = getWindowVars(text, vars)
        const extern = buildVarsOption([...vars, ...windowVars])
        event('runQuery', {context: 'TimeSeries'})
        if (
          isCurrentPageDashboard &&
          isFlagEnabled('queryCacheForDashboards')
        ) {
          return onGetCachedResultsThunk(orgID, text)
        }
        const queryID = hashCode(text)
        if (!this.hashMapMutex[queryID]) {
          this.hashMapMutex[queryID] = RunQueryPromiseMutex<RunQueryResult>()
        }
        return this.hashMapMutex[queryID].run(orgID, text, extern) as CancelBox<
          RunQueryResult
        >
      })

      // Wait for new queries to complete
      const results = await Promise.all(this.pendingResults.map(r => r.promise))

      let statuses = [] as StatusRow[][]
      if (check) {
        const extern = buildVarsOption(vars)
        this.pendingCheckStatuses = runStatusesQuery(
          this.props.match.params.orgID,
          check.id,
          extern
        )
        statuses = await this.pendingCheckStatuses.promise // TODO handle errors
      }

      const duration = Date.now() - startTime

      for (const result of results) {
        if (result.type === 'UNKNOWN_ERROR') {
          if (isDemoDataAvailabilityError(result.code, result.message)) {
            notify(
              demoDataAvailability(demoDataError(this.props.match.params.orgID))
            )
          }
          errorMessage = result.message
          throw new Error(result.message)
        }

        if (result.type === 'RATE_LIMIT_ERROR') {
          errorMessage = result.message
          notify(rateLimitReached(result.retryAfter))

          throw new Error(result.message)
        }

        if (result.didTruncate) {
          notify(resultTooLarge(result.bytesRead))
        }
      }

      const files = (results as RunQuerySuccessResult[]).map(r => r.csv)
      let giraffeResult

      if (isFlagEnabled('fluxParser')) {
        giraffeResult = fromFlux(files.join('\n\n'))
      } else {
        giraffeResult = fromFluxGiraffe(files.join('\n\n'))
      }

      this.pendingReload = false

      this.setState({
        giraffeResult,
        errorMessage,
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
    if (this.props.hasUpdatedTimeRangeInVEO) {
      return false
    }

    if (
      prevProps.hasUpdatedTimeRangeInVEO &&
      !this.props.hasUpdatedTimeRangeInVEO
    ) {
      return true
    }

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

const mstp = (state: AppState, props: OwnProps) => {
  const timeRange = getTimeRange(state)

  // NOTE: cannot use getAllVariables here because the TimeSeries
  // component appends it automatically. That should be fixed
  // NOTE: limit the variables returned to those that are used,
  // as this prevents resending when other queries get sent
  const queries = props.queries
    ? props.queries.map(q => q.text).filter(t => !!t.trim())
    : []
  const vars = getVariables(state).filter(v =>
    queries.some(t => isInQuery(t, v))
  )
  const variables = [
    ...vars,
    getRangeVariable(TIME_RANGE_START, timeRange),
    getRangeVariable(TIME_RANGE_STOP, timeRange),
  ]

  return {
    hasUpdatedTimeRangeInVEO: hasUpdatedTimeRangeInVEO(state),
    isCurrentPageDashboard: isCurrentPageDashboardSelector(state),
    queryLink: state.links.query.self,
    buckets: getAll<Bucket>(state, ResourceType.Buckets),
    variables,
  }
}

const mdtp = {
  notify: notifyAction,
  onGetCachedResultsThunk: getCachedResultsThunk,
  setCellMount: setCellMountAction,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(TimeSeries))
