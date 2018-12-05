// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import LineProtocolTabs from 'src/onboarding/components/configureStep/lineProtocol/LineProtocolTabs'
import LoadingState from 'src/onboarding/components/configureStep/lineProtocol/LoadingState'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {LineProtocolTab, LineProtocolStatus} from 'src/types/v2/dataLoaders'

interface Props {
  bucket: string
  org: string
}

interface State {
  activeCard: LineProtocolStatus
}

@ErrorHandling
class LineProtocol extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {activeCard: LineProtocolStatus.ImportData}
  }
  public render() {
    const {activeCard} = this.state
    const {bucket, org} = this.props
    return (
      <>
        <h3 className="wizard-step--title">Add Data via Line Protocol</h3>
        <h5 className="wizard-step--sub-title">
          Need help writing InfluxDB Line Protocol? See Documentation
        </h5>
        {activeCard === LineProtocolStatus.ImportData ? (
          <LineProtocolTabs
            tabs={this.LineProtocolTabs}
            bucket={bucket}
            org={org}
          />
        ) : (
          <LoadingState activeCard={activeCard} />
        )}
      </>
    )
  }
  private get LineProtocolTabs(): LineProtocolTab[] {
    return [
      LineProtocolTab.UploadFile,
      LineProtocolTab.EnterManually,
      LineProtocolTab.EnterURL,
    ]
  }
}

export default LineProtocol
