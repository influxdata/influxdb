// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import ViewLoadingSpinner from 'src/shared/components/ViewLoadingSpinner'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {getTimeRange} from 'src/dashboards/selectors'
import {checkResultsLength} from 'src/shared/utils/vis'
import {getActiveTimeRange} from 'src/timeMachine/selectors/index'

// Types
import {
  TimeRange,
  TimeZone,
  AppState,
  DashboardQuery,
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
    const {ranges, properties, manualRefresh, timeZone, theme} = this.props
    const {submitToken} = this.state

    return (
      <TimeSeries
        submitToken={submitToken}
        queries={this.queries}
        key={manualRefresh}
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
            <>
              <ViewLoadingSpinner loading={loading} />
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
                  properties={properties}
                  timeRange={ranges}
                  statuses={statuses}
                  timeZone={timeZone}
                  theme={theme}
                />
              </EmptyQueryView>
            </>
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
  const ranges = getActiveTimeRange(timeRange, ownProps.properties.queries)
  const {timeZone, theme} = state.app.persisted

  return {
    timeRange,
    ranges,
    timeZone,
    theme,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(
  withRouter(RefreshingView)
)
