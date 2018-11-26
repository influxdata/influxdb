// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TimeSeries from 'src/shared/components/TimeSeries'
import EmptyRefreshingView from 'src/shared/components/EmptyRefreshingView'
import RefreshingViewSwitcher from 'src/shared/components/RefreshingViewSwitcher'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'

// Types
import {TimeRange} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'
import {RefreshingViewProperties, ViewType} from 'src/types/v2/dashboards'

interface Props {
  timeRange: TimeRange
  viewID: string
  inView: boolean
  manualRefresh: number
  onZoom: (range: TimeRange) => void
  properties: RefreshingViewProperties
}

class RefreshingView extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    inView: true,
    manualRefresh: 0,
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

    return (
      <TimeSeries
        inView={inView}
        autoRefresher={GlobalAutoRefresher}
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
              queries={this.queries}
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

export default RefreshingView
