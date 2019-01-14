// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import _ from 'lodash'

// Components
import LineProtocolTabs from 'src/onboarding/components/configureStep/lineProtocol/LineProtocolTabs'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import {Form} from 'src/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  setLPStatus as setLPStatusAction,
  writeLineProtocolAction,
} from 'src/onboarding/actions/dataLoaders'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'
import {AppState} from 'src/types/v2/index'
import {WritePrecision} from 'src/api'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  bucket: string
  org: string
  onClickNext: () => void
  onClickBack: () => void
  onClickSkip: () => void
}

interface StateProps {
  lineProtocolBody: string
  precision: WritePrecision
}

interface DispatchProps {
  setLPStatus: typeof setLPStatusAction
  writeLineProtocolAction: typeof writeLineProtocolAction
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
export class LineProtocol extends PureComponent<Props> {
  public componentDidMount() {
    const {setLPStatus} = this.props
    setLPStatus(RemoteDataState.NotStarted)
  }

  public render() {
    return (
      <Form onSubmit={this.handleSubmit}>
        <div className="wizard-step--scroll-area">
          <FancyScrollbar autoHide={false}>
            <div className="wizard-step--scroll-content">
              <h3 className="wizard-step--title">Add Data via Line Protocol</h3>
              <h5 className="wizard-step--sub-title">
                Need help writing InfluxDB Line Protocol? See Documentation
              </h5>
              {this.Content}
            </div>
          </FancyScrollbar>
        </div>
        <OnboardingButtons
          onClickBack={this.props.onClickBack}
          onClickSkip={this.props.onClickSkip}
          showSkip={true}
          autoFocusNext={true}
          skipButtonText={'Skip Config'}
        />
      </Form>
    )
  }

  private get LineProtocolTabs(): LineProtocolTab[] {
    return [LineProtocolTab.UploadFile, LineProtocolTab.EnterManually]
  }

  private get Content(): JSX.Element {
    const {bucket, org} = this.props
    return (
      <LineProtocolTabs
        tabs={this.LineProtocolTabs}
        bucket={bucket}
        org={org}
        handleSubmit={this.handleSubmit}
      />
    )
  }

  private handleSubmit = () => {
    const {
      bucket,
      org,
      writeLineProtocolAction,
      lineProtocolBody,
      precision,
    } = this.props

    writeLineProtocolAction(org, bucket, lineProtocolBody, precision)
    this.props.onClickNext()
  }
}

const mstp = ({
  onboarding: {
    dataLoaders: {lineProtocolBody, precision},
  },
}: AppState): StateProps => {
  return {lineProtocolBody, precision}
}

const mdtp: DispatchProps = {
  setLPStatus: setLPStatusAction,
  writeLineProtocolAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocol)
