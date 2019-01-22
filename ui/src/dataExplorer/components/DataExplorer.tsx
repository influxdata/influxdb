// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setActiveTimeMachine} from 'src/shared/actions/v2/timeMachines'

// Utils
import {DE_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/shared/apis/v2/queryBuilder'

// Styles
import './DataExplorer.scss'

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

class DataExplorer extends PureComponent<DispatchProps, {}> {
  constructor(props: DispatchProps) {
    super(props)

    props.onSetActiveTimeMachine(DE_TIME_MACHINE_ID)
    queryBuilderFetcher.clearCache()
  }

  public render() {
    return (
      <div className="data-explorer">
        <HoverTimeProvider>
          <TimeMachine />
        </HoverTimeProvider>
      </div>
    )
  }
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(DataExplorer)
