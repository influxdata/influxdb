// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import _ from 'lodash'

// Components
import LineProtocolTabs from 'src/onboarding/components/configureStep/lineProtocol/LineProtocolTabs'
import LoadingStatusIndicator from 'src/onboarding/components/configureStep/lineProtocol/LoadingStatusIndicator'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import {Form} from 'src/clockface'

// Actions
import {setLPStatus as setLPStatusAction} from 'src/onboarding/actions/dataLoaders'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'
import {AppState} from 'src/types/v2/index'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  bucket: string
  org: string
  onClickNext: () => void
  onClickBack: () => void
  onClickSkip: () => void
}

interface StateProps {
  lpStatus: RemoteDataState
}

interface DispatchProps {
  setLPStatus: typeof setLPStatusAction
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
export class LineProtocol extends PureComponent<Props> {
  public render() {
    return (
      <Form onSubmit={this.props.onClickNext}>
        <h3 className="wizard-step--title">Add Data via Line Protocol</h3>
        <h5 className="wizard-step--sub-title">
          Need help writing InfluxDB Line Protocol? See Documentation
        </h5>
        {this.Content}
        <OnboardingButtons
          nextButtonText={this.nextButtonText}
          backButtonText={this.backButtonText}
          onClickBack={this.props.onClickBack}
          onClickSkip={this.props.onClickSkip}
          showSkip={true}
          autoFocusNext={true}
          skipButtonText={'Skip Config'}
        />
      </Form>
    )
  }

  private get nextButtonText(): string {
    return 'Continue to Verify'
  }

  private get backButtonText(): string {
    return 'Back to Select Data Source Type'
  }

  private get LineProtocolTabs(): LineProtocolTab[] {
    return [
      LineProtocolTab.UploadFile,
      LineProtocolTab.EnterManually,
      LineProtocolTab.EnterURL,
    ]
  }

  private get Content(): JSX.Element {
    const {bucket, org, lpStatus} = this.props
    if (lpStatus === RemoteDataState.NotStarted) {
      return (
        <LineProtocolTabs
          tabs={this.LineProtocolTabs}
          bucket={bucket}
          org={org}
        />
      )
    }
    return (
      <LoadingStatusIndicator
        status={lpStatus}
        onClickRetry={this.handleRetry}
      />
    )
  }

  private handleRetry = () => {
    const {setLPStatus} = this.props
    setLPStatus(RemoteDataState.NotStarted)
  }
}

const mstp = ({
  onboarding: {
    dataLoaders: {lpStatus},
  },
}: AppState): StateProps => {
  return {lpStatus}
}

const mdtp: DispatchProps = {
  setLPStatus: setLPStatusAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocol)
