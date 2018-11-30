import React, {SFC} from 'react'

import 'src/shared/components/TimeMachineQueryBuilder.scss'

interface Props {}

const TimeMachineQueryBuilder: SFC<Props> = () => {
  return (
    <div className="query-builder">
      <div className="query-builder--panel">
        <div className="query-builder--panel-header">Select a Bucket</div>
        <div className="query-builder--panel-body" />
      </div>
      <div className="query-builder--panel">
        <div className="query-builder--panel-header">Select a Measurement</div>
        <div className="query-builder--panel-body" />
      </div>
      <div className="query-builder--panel">
        <div className="query-builder--panel-header">
          Select Measurement Fields
        </div>
        <div className="query-builder--panel-body" />
      </div>
      <div className="query-builder--panel">
        <div className="query-builder--panel-header">Select Statistic</div>
        <div className="query-builder--panel-body" />
      </div>
    </div>
  )
}

export default TimeMachineQueryBuilder
