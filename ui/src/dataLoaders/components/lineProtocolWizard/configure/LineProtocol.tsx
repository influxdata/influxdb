// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import _ from 'lodash'

// Components
import {Form} from '@influxdata/clockface'
import LineProtocolTabs from 'src/dataLoaders/components/lineProtocolWizard/configure/LineProtocolTabs'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  setLPStatus as setLPStatusAction,
  writeLineProtocolAction,
} from 'src/dataLoaders/actions/dataLoaders'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {LineProtocolTab} from 'src/types/dataLoaders'
import {AppState} from 'src/types/index'
import {WritePrecision} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'
import {LineProtocolStepProps} from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'

type OwnProps = LineProtocolStepProps

interface StateProps {
  lineProtocolBody: string
  precision: WritePrecision
  bucket: string
  org: string
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
      <div className="onboarding-step">
        <Form onSubmit={this.handleSubmit}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <h3 className="wizard-step--title">
                  Add Data via Line Protocol
                </h3>
                <h5 className="wizard-step--sub-title">
                  Need help writing InfluxDB Line Protocol? See Documentation
                </h5>
                {this.content}
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons autoFocusNext={true} />
        </Form>
      </div>
    )
  }

  private get LineProtocolTabs(): LineProtocolTab[] {
    return [LineProtocolTab.UploadFile, LineProtocolTab.EnterManually]
  }

  private get content(): JSX.Element {
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

  private handleSubmit = async () => {
    const {
      bucket,
      org,
      writeLineProtocolAction,
      lineProtocolBody,
      precision,
      onIncrementCurrentStepIndex,
    } = this.props

    writeLineProtocolAction(org, bucket, lineProtocolBody, precision)
    onIncrementCurrentStepIndex()
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {lineProtocolBody, precision},
    steps: {bucket, org},
  },
}: AppState): StateProps => {
  return {lineProtocolBody, precision, bucket, org}
}

const mdtp: DispatchProps = {
  setLPStatus: setLPStatusAction,
  writeLineProtocolAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocol)
