// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyQueryView from 'src/shared/components/EmptyQueryView'
import QueryViewSwitcher from 'src/shared/components/QueryViewSwitcher'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'

// Types
import {TimeRange} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'
import {QueryViewProperties, ViewType} from 'src/types/v2/dashboards'

interface Props {
  timeRange: TimeRange
  viewID: string
  inView: boolean
  manualRefresh: number
  onZoom: (range: TimeRange) => void
  properties: QueryViewProperties
}

interface State {
  submitToken: number
}

class RefreshingView extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
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
      inView,
      onZoom,
      viewID,
      timeRange,
      properties,
      manualRefresh,
    } = this.props
    const {submitToken} = this.state

    return (
      <TimeSeries
        inView={inView}
        submitToken={submitToken}
        queries={this.queries}
        key={manualRefresh}
        variables={getTimeRangeVars(timeRange)}
      >
        {({tables, loading, error, isInitialFetch}) => {
          return (
            <EmptyQueryView
              error={error}
              tables={tables}
              loading={loading}
              isInitialFetch={isInitialFetch}
              queries={this.queries}
              fallbackNote={this.fallbackNote}
            >
              <QueryViewSwitcher
                tables={tables}
                viewID={viewID}
                onZoom={onZoom}
                loading={loading}
                timeRange={timeRange}
                properties={properties}
              />
            </EmptyQueryView>
          )
        }}
      </TimeSeries>
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

  private get fallbackNote(): string {
    const {note, showNoteWhenEmpty} = this.props.properties

    return showNoteWhenEmpty ? note : null
  }

  private incrementSubmitToken = () => {
    this.setState({submitToken: Date.now()})
  }
}

export default RefreshingView
