// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import GetVariables from 'src/shared/components/GetVariables'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Utils
import {DE_TIME_MACHINE_ID} from 'src/timeMachine/constants'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'

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
          <GetVariables>
            <TimeMachine />
          </GetVariables>
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
