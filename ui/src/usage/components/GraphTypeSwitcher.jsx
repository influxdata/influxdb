import React, {Component} from 'react'

import SparkLine from 'js/components/SparkLine'
import SingleStat from 'js/components/SingleStat'
import EmptyGraph from 'js/components/EmptyGraph'

import {
  QUERY_RESULTS_STATUS_ERROR,
  QUERY_RESULTS_STATUS_EMPTY,
  QUERY_RESULTS_STATUS_TIMEOUT,
} from 'js/components/Usage/Constants'

export default class GraphTypeSwitcher extends Component {
  render() {
    const {graphInfo, status, table} = this.props

    switch (status) {
      case QUERY_RESULTS_STATUS_ERROR:
        return <EmptyGraph title={graphInfo.title} isError={true} />

      case QUERY_RESULTS_STATUS_TIMEOUT:
        return (
          <EmptyGraph
            title={graphInfo.title}
            isError={true}
            errorMessage="Query has timed out"
          />
        )

      case QUERY_RESULTS_STATUS_EMPTY:
        return <EmptyGraph title={graphInfo.title} isError={false} />

      default:
        if (graphInfo.type === 'sparkline') {
          return <SparkLine {...graphInfo} table={table} />
        }

        if (graphInfo.type === 'stat') {
          return <SingleStat {...graphInfo} table={table} />
        }

        return <div />
    }
  }
}
