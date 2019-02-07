// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import DataStreaming from 'src/dataLoaders/components/verifyStep/DataStreaming'
import FetchAuthToken from 'src/dataLoaders/components/verifyStep/FetchAuthToken'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  setActiveTelegrafPlugin,
  setPluginConfiguration,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {CollectorsStepProps} from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import {Form} from 'src/clockface'
import {AppState} from 'src/types/v2'

type OwnProps = CollectorsStepProps

interface StateProps {
  username: string
  telegrafConfigID: string
  bucket: string
  org: string
}

interface DispatchProps {
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
}

export type Props = DispatchProps & StateProps & OwnProps

@ErrorHandling
export class VerifyCollectorStep extends PureComponent<Props> {
  public render() {
    const {
      username,
      telegrafConfigID,
      bucket,
      notify,
      org,
      onDecrementCurrentStepIndex,
      onExit,
    } = this.props

    return (
      <div className="onboarding-step">
        <h3 className="wizard-step--title">Test your Configuration</h3>
        <h5 className="wizard-step--sub-title">
          Start Telegraf and ensure data is being written to InfluxDB
        </h5>
        <Form onSubmit={onExit}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <FetchAuthToken bucket={bucket} username={username}>
                  {authToken => (
                    <DataStreaming
                      notify={notify}
                      org={org}
                      configID={telegrafConfigID}
                      authToken={authToken}
                      bucket={bucket}
                    />
                  )}
                </FetchAuthToken>
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={onDecrementCurrentStepIndex}
            nextButtonText={'Finish'}
          />
        </Form>
      </div>
    )
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafConfigID},
    steps: {bucket, org},
  },
  me: {name},
}: AppState): StateProps => ({
  username: name,
  telegrafConfigID,
  bucket,
  org,
})

const mdtp: DispatchProps = {
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VerifyCollectorStep)
