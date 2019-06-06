// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyQueryView from 'src/shared/components/EmptyQueryView'
import RefreshingViewSwitcher from 'src/shared/components/RefreshingViewSwitcher'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {getVariableAssignments} from 'src/variables/selectors'
import {getDashboardValuesStatus} from 'src/variables/selectors'

// Types
import {TimeRange, RemoteDataState} from 'src/types'
import {VariableAssignment} from 'src/types/ast'
import {AppState} from 'src/types'
import {DashboardQuery} from 'src/types/dashboards'
import {QueryViewProperties, ViewType} from 'src/types/dashboards'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

interface OwnProps {
  timeRange: TimeRange
  manualRefresh: number
  properties: QueryViewProperties
  dashboardID: string
}

interface StateProps {
  variableAssignments: VariableAssignment[]
  variablesStatus: RemoteDataState
}

interface State {
  submitToken: number
}

type Props = OwnProps & StateProps

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
    const {properties, manualRefresh, variablesStatus} = this.props
    const {submitToken} = this.state

    return (
      <SpinnerContainer
        loading={variablesStatus}
        spinnerComponent={<TechnoSpinner />}
      >
        <TimeSeries
          submitToken={submitToken}
          queries={this.queries}
          key={manualRefresh}
          variables={this.variableAssignments}
        >
          {({tables, files, loading, errorMessage, isInitialFetch}) => {
            return (
              <EmptyQueryView
                errorMessage={errorMessage}
                tables={tables}
                loading={loading}
                isInitialFetch={isInitialFetch}
                queries={this.queries}
                fallbackNote={this.fallbackNote}
              >
                <RefreshingViewSwitcher
                  tables={tables}
                  files={files}
                  loading={loading}
                  properties={properties}
                />
              </EmptyQueryView>
            )
          }}
        </TimeSeries>
      </SpinnerContainer>
    )
  }

  private get queries(): DashboardQuery[] {
    const {properties} = this.props
    const {type, queries} = properties

    if (type === ViewType.SingleStat) {
      return [queries[0]]
    }

    if (type === ViewType.Gauge) {
      return [queries[0]]
    }

    return queries
  }

  private get variableAssignments(): VariableAssignment[] {
    const {timeRange, variableAssignments} = this.props

    return [...variableAssignments, ...getTimeRangeVars(timeRange)]
  }

  private get fallbackNote(): string {
    const {note, showNoteWhenEmpty} = this.props.properties

    return showNoteWhenEmpty ? note : null
  }

  private incrementSubmitToken = () => {
    this.setState({submitToken: Date.now()})
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const variableAssignments = getVariableAssignments(
    state,
    ownProps.dashboardID
  )

  const valuesStatus = getDashboardValuesStatus(state, ownProps.dashboardID)

  return {variableAssignments, variablesStatus: valuesStatus}
}

export default connect<StateProps, {}, OwnProps>(mstp)(RefreshingView)
