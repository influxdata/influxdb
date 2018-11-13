// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import EmptyRefreshingView from 'src/shared/components/EmptyRefreshingView'
import RefreshingViewSwitcher from 'src/shared/components/RefreshingViewSwitcher'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'

// Utils
import {getActiveSource} from 'src/sources/selectors'

// Types
import {TimeRange} from 'src/types'
import {AppState} from 'src/types/v2'
import {DashboardQuery} from 'src/types/v2/dashboards'
import {RefreshingViewProperties, ViewType} from 'src/types/v2/dashboards'

interface OwnProps {
  timeRange: TimeRange
  viewID: string
  inView: boolean
  manualRefresh: number
  onZoom: (range: TimeRange) => void
  properties: RefreshingViewProperties
}

interface StateProps {
  link: string
}

interface DispatchProps {}

type Props = OwnProps & StateProps & DispatchProps

class RefreshingView extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    inView: true,
    manualRefresh: 0,
  }

  public render() {
    const {
      link,
      inView,
      onZoom,
      viewID,
      timeRange,
      properties,
      manualRefresh,
    } = this.props

    if (!properties.queries.length) {
      return <EmptyGraphMessage message={emptyGraphCopy} />
    }

    return (
      <TimeSeries
        link={link}
        inView={inView}
        queries={this.queries}
        key={manualRefresh}
      >
        {({tables, loading, error, isInitialFetch}) => {
          return (
            <EmptyRefreshingView
              error={error}
              tables={tables}
              loading={loading}
              isInitialFetch={isInitialFetch}
            >
              <RefreshingViewSwitcher
                tables={tables}
                viewID={viewID}
                onZoom={onZoom}
                loading={loading}
                timeRange={timeRange}
                properties={properties}
              />
            </EmptyRefreshingView>
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
}

const mstp = (state: AppState): StateProps => {
  const link = getActiveSource(state).links.query

  return {link}
}

const mdtp = {}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(RefreshingView)
