// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Utils
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

type Props = DispatchProps
class DataExplorer extends PureComponent<Props, {}> {
  constructor(props: Props) {
    super(props)

    props.onSetActiveTimeMachine('de')
    queryBuilderFetcher.clearCache()
  }

  public render() {
    return (
      <LimitChecker>
        <RateLimitAlert />
        <div className="data-explorer">
          <HoverTimeProvider>
            <TimeMachine />
          </HoverTimeProvider>
        </div>
      </LimitChecker>
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
