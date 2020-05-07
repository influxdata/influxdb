import React, {FC} from 'react'

import SparkLine from './SparkLine'
import SingleStat from './SingleStat'
import EmptyGraph from './EmptyGraph'

import {UsageGraphInfo, UsageTable} from 'src/types'

interface Props {
  graphInfo: UsageGraphInfo
  table: UsageTable
}

const GraphTypeSwitcher: FC<Props> = ({graphInfo, table}) => {
  if (!table) {
    return <EmptyGraph title={graphInfo.title} isError={false} />
  }

  switch (status) {
    case 'error':
      return <EmptyGraph title={graphInfo.title} isError={true} />

    case 'timeout':
      return (
        <EmptyGraph
          title={graphInfo.title}
          isError={true}
          errorMessage="Query has timed out"
        />
      )

    case 'empty':
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

export default GraphTypeSwitcher
