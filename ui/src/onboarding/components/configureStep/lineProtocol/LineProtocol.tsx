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

interface State {
  activeCard: LineProtocolStatus
}

@ErrorHandling
class LineProtocol extends PureComponent<{}, State> {
  constructor(props) {
    super(props)
    this.state = {activeCard: LineProtocolStatus.ImportData}
  }
  public render() {
    const {activeCard} = this.state
    return (
      <>
        <h3 className="wizard-step--title">Add Data via Line Protocol</h3>
        <h5 className="wizard-step--sub-title">
          Need help writing InfluxDB Line Protocol? See Documentation
        </h5>
        {activeCard === LineProtocolStatus.ImportData ? (
          <LineProtocolTabs tabs={this.LineProtocolTabs} />
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
