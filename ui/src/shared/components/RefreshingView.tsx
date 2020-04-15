// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {getTimeRange} from 'src/dashboards/selectors'
import {getRangeVariable} from 'src/variables/utils/getTimeRangeVars'
import {getVariables, asAssignment} from 'src/variables/selectors'
import {checkResultsLength} from 'src/shared/utils/vis'
import {getActiveTimeRange} from 'src/timeMachine/selectors/index'
import {TIME_RANGE_START, TIME_RANGE_STOP} from 'src/variables/constants'

// Types
import {
  TimeRange,
  TimeZone,
  AppState,
  DashboardQuery,
  VariableAssignment,
  QueryViewProperties,
  Theme,
} from 'src/types'

interface OwnProps {
  manualRefresh: number
  properties: QueryViewProperties
}

interface StateProps {
  theme: Theme
  timeRange: TimeRange
  ranges: TimeRange | null
  timeZone: TimeZone
  variableAssignments: VariableAssignment[]
}

interface State {
  submitToken: number
}

type Props = OwnProps & StateProps & WithRouterProps

class RefreshingView extends PureComponent<Props, State> {
  public static defaultProps = {
    inView: true,
    manualRefresh: 0,
  }

  constructor(props) {
    super(props)

    this.state = {submitToken: 0}
  }

  public componentDidMount() {
    GlobalAutoRefresher.subscribe(this.incrementSubmitToken)
  }

  public componentWillUnmount() {
    GlobalAutoRefresher.unsubscribe(this.incrementSubmitToken)
  }

  public render() {
    const {
      ranges,
      properties,
      manualRefresh,
      timeZone,
      variableAssignments,
      theme,
    } = this.props
    const {submitToken} = this.state

    return (
      <TimeSeries
        submitToken={submitToken}
        queries={this.queries}
        key={manualRefresh}
        variables={variableAssignments}
      >
        {({
          giraffeResult,
          files,
          loading,
          errorMessage,
          isInitialFetch,
          statuses,
        }) => {
          return (
            <EmptyQueryView
              errorFormat={ErrorFormat.Scroll}
              errorMessage={errorMessage}
              hasResults={checkResultsLength(giraffeResult)}
              loading={loading}
              isInitialFetch={isInitialFetch}
              queries={this.queries}
              fallbackNote={this.fallbackNote}
            >
              <ViewSwitcher
                files={files}
                giraffeResult={giraffeResult}
                loading={loading}
                properties={properties}
                timeRange={ranges}
                statuses={statuses}
                timeZone={timeZone}
                theme={theme}
              />
            </EmptyQueryView>
          )
        }}
      </TimeSeries>
    )
  }

  private get queries(): DashboardQuery[] {
    const {properties} = this.props

    switch (properties.type) {
      case 'single-stat':
      case 'gauge':
        return [properties.queries[0]]
      default:
        return properties.queries
    }
  }

  private get fallbackNote(): string {
    const {properties} = this.props

    switch (properties.type) {
      case 'check':
        return null
      default:
        const {note, showNoteWhenEmpty} = properties

        return showNoteWhenEmpty ? note : null
    }
  }

  private incrementSubmitToken = () => {
    this.setState({submitToken: Date.now()})
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const timeRange = getTimeRange(state)

  // NOTE: cannot use getAllVariables here because the TimeSeries
  // component appends it automatically. That should be fixed
  const vars = getVariables(state)
  const variableAssignments = [
    ...vars,
    getRangeVariable(TIME_RANGE_START, timeRange),
    getRangeVariable(TIME_RANGE_STOP, timeRange),
  ]
    .map(v => asAssignment(v))
    .filter(v => !!v)

  const ranges = getActiveTimeRange(timeRange, ownProps.properties.queries)
  const {timeZone, theme} = state.app.persisted

  return {
    timeRange,
    ranges,
    timeZone,
    variableAssignments,
    theme,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(
  withRouter(RefreshingView)
)
