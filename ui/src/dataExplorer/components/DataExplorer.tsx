// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import LimitChecker from 'src/cloud/components/LimitChecker'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Utils
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'
import {
  extractRateLimitResourceName,
  extractRateLimitStatus,
} from 'src/cloud/utils/limits'

// Types
import {AppState} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'

interface StateProps {
  resourceName: string
  limitStatus: LimitStatus
}

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

type Props = DispatchProps & StateProps
class DataExplorer extends PureComponent<Props, {}> {
  constructor(props: Props) {
    super(props)

    props.onSetActiveTimeMachine('de')
    queryBuilderFetcher.clearCache()
  }

  public render() {
    const {resourceName, limitStatus} = this.props

    return (
      <LimitChecker>
        <AssetLimitAlert
          resourceName={resourceName}
          limitStatus={limitStatus}
        />
        <div className="data-explorer">
          <HoverTimeProvider>
            <TimeMachine />
          </HoverTimeProvider>
        </div>
      </LimitChecker>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    cloud: {limits},
  } = state

  return {
    resourceName: extractRateLimitResourceName(limits),
    limitStatus: extractRateLimitStatus(limits),
  }
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(DataExplorer)
