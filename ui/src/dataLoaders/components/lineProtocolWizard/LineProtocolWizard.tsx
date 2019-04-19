// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
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
import {AppState} from 'src/types'
import {Bucket} from '@influxdata/influx'

export interface LineProtocolStepProps {
  currentStepIndex: number
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  notify: (message: Notification | NotificationFunc) => void
  onExit: () => void
}

interface OwnProps {
  onCompleteSetup: () => void
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
  buckets: Bucket[]
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class LineProtocolWizard extends PureComponent<Props & WithRouterProps> {
  public componentDidMount() {
    this.handleSetBucketInfo()
    this.handleSetStartingValues()
  }

  public render() {
    const {buckets} = this.props

    return (
      <WizardOverlay title="Add Line Protocol" onDismiss={this.handleDismiss}>
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
      const {organizationID, name, id} = buckets[0]

      this.props.onSetBucketInfo(organizationID, name, id)
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
    const {router, onClearDataLoaders, onClearSteps} = this.props

    onClearDataLoaders()
    onClearSteps()
    router.goBack()
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
  buckets,
}: AppState): StateProps => ({
  currentStepIndex: currentStep,
  username: name,
  bucket,
  buckets: buckets.list,
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
)(withRouter<Props>(LineProtocolWizard))
