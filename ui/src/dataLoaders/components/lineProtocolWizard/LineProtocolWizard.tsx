// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import LineProtocolStepSwitcher from 'src/dataLoaders/components/lineProtocolWizard/verify/LineProtocolStepSwitcher'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  setBucketInfo,
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
  setCurrentStepIndex,
  clearSteps,
} from 'src/dataLoaders/actions/steps'

import {clearDataLoaders} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'
import {Bucket} from 'src/api'

export interface LineProtocolStepProps {
  currentStepIndex: number
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  notify: (message: Notification | NotificationFunc) => void
  onExit: () => void
}

interface OwnProps {
  onCompleteSetup: () => void
  visible: boolean
  buckets: Bucket[]
  startingStep?: number
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetBucketInfo: typeof setBucketInfo
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onClearDataLoaders: typeof clearDataLoaders
  onClearSteps: typeof clearSteps
}

interface StateProps {
  currentStepIndex: number
  username: string
  bucket: string
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class CollectorsWizard extends PureComponent<Props> {
  public componentDidMount() {
    this.handleSetBucketInfo()
    this.handleSetStartingValues()
  }

  public componentDidUpdate(prevProps: Props) {
    const hasBecomeVisible = !prevProps.visible && this.props.visible

    if (hasBecomeVisible) {
      this.handleSetBucketInfo()
      this.handleSetStartingValues()
    }
  }

  public render() {
    const {visible, buckets} = this.props

    return (
      <WizardOverlay
        visible={visible}
        title={'Add Line Protocol'}
        onDismis={this.handleDismiss}
      >
        <div className="wizard-contents">
          <div className="wizard-step--container">
            <LineProtocolStepSwitcher
              stepProps={this.stepProps}
              buckets={buckets}
            />
          </div>
        </div>
      </WizardOverlay>
    )
  }

  private handleSetBucketInfo = () => {
    const {bucket, buckets} = this.props
    if (!bucket && (buckets && buckets.length)) {
      const {organization, organizationID, name, id} = buckets[0]

      this.props.onSetBucketInfo(organization, organizationID, name, id)
    }
  }

  private handleSetStartingValues = () => {
    const {startingStep} = this.props

    const hasStartingStep = startingStep || startingStep === 0

    if (hasStartingStep) {
      this.props.onSetCurrentStepIndex(startingStep)
    }
  }

  private handleDismiss = () => {
    this.props.onCompleteSetup()
    this.props.onClearDataLoaders()
    this.props.onClearSteps()
  }

  private get stepProps(): LineProtocolStepProps {
    const {
      notify,
      currentStepIndex,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return {
      currentStepIndex,
      onIncrementCurrentStepIndex,
      onDecrementCurrentStepIndex,
      notify,
      onExit: this.handleDismiss,
    }
  }
}

const mstp = ({
  dataLoading: {
    steps: {currentStep, bucket},
  },
  me: {name},
}: AppState): StateProps => ({
  currentStepIndex: currentStep,
  username: name,
  bucket,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetBucketInfo: setBucketInfo,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onClearDataLoaders: clearDataLoaders,
  onClearSteps: clearSteps,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CollectorsWizard)
