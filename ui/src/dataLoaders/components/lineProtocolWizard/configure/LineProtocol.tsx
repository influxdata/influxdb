// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import _ from 'lodash'

// Components
import {Form, Overlay} from '@influxdata/clockface'
import LineProtocolTabs from 'src/dataLoaders/components/lineProtocolWizard/configure/LineProtocolTabs'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

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
    const {bucket, org} = this.props

    return (
      <Form onSubmit={this.handleSubmit}>
        <Overlay.Body style={{textAlign: 'center'}}>
          <LineProtocolTabs
            tabs={this.LineProtocolTabs}
            bucket={bucket}
            org={org}
          />
          <p>
            Need help writing InfluxDB Line Protocol?{' '}
            <a
              href="https://v2.docs.influxdata.com/v2.0/write-data/#write-data-in-the-influxdb-ui"
              target="_blank"
            >
              See Documentation
            </a>
          </p>
        </Overlay.Body>
        <OnboardingButtons autoFocusNext={true} nextButtonText="Write Data" />
      </Form>
    )
  }

  private get LineProtocolTabs(): LineProtocolTab[] {
    return [LineProtocolTab.UploadFile, LineProtocolTab.EnterManually]
  }

  private handleSubmit = () => {
    const {onIncrementCurrentStepIndex} = this.props
    this.handleUpload()
    onIncrementCurrentStepIndex()
  }

  private handleUpload = () => {
    const {bucket, org, lineProtocolBody, precision} = this.props
    this.props.writeLineProtocolAction(org, bucket, lineProtocolBody, precision)
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {lineProtocolBody, precision},
    steps: {bucket},
  },
  orgs,
}: AppState): StateProps => {
  return {lineProtocolBody, precision, bucket, org: orgs.org.name}
}

const mdtp: DispatchProps = {
  setLPStatus: setLPStatusAction,
  writeLineProtocolAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocol)
