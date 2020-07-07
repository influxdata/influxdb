// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Form, Overlay} from '@influxdata/clockface'
import LineProtocolTabs from 'src/dataLoaders/components/lineProtocolWizard/configure/LineProtocolTabs'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import LineProtocolHelperText from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolHelperText'

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
import {RemoteDataState} from 'src/types'
import {LineProtocolStepProps} from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'

// Selectors
import {getOrg} from 'src/organizations/selectors'

type OwnProps = LineProtocolStepProps

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

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
          <LineProtocolHelperText />
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

const mstp = (state: AppState) => {
  const {dataLoading} = state
  const {
    dataLoaders: {lineProtocolBody, precision},
    steps: {bucket},
  } = dataLoading
  const org = getOrg(state).name

  return {lineProtocolBody, precision, bucket, org}
}

const mdtp = {
  setLPStatus: setLPStatusAction,
  writeLineProtocolAction,
}

const connector = connect(mstp, mdtp)

export default connector(LineProtocol)
