// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import StepSwitcher from 'src/dataLoaders/components/StepSwitcher'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  setBucketInfo,
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
  setCurrentStepIndex,
  setSubstepIndex,
  clearSteps,
} from 'src/dataLoaders/actions/steps'

import {
  setDataLoadersType,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {Links} from 'src/types/v2/links'
import {DataLoaderType, Substep} from 'src/types/v2/dataLoaders'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'
import {Bucket} from '@influxdata/influx'

export interface DataLoaderStepProps {
  links: Links
  currentStepIndex: number
  substep: Substep
  onSetCurrentStepIndex: (stepNumber: number) => void
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  onSetSubstepIndex: (index: number, subStep: Substep) => void
  notify: (message: Notification | NotificationFunc) => void
  onCompleteSetup: () => void
  onExit: () => void
}

interface OwnProps {
  onCompleteSetup: () => void
  visible: boolean
  buckets: Bucket[]
  startingType?: DataLoaderType
  startingStep?: number
  startingSubstep?: Substep
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetBucketInfo: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onSetSubstepIndex: typeof setSubstepIndex
  onClearDataLoaders: typeof clearDataLoaders
  onClearSteps: typeof clearSteps
}

interface StateProps {
  links: Links
  currentStepIndex: number
  substep: Substep
  username: string
  bucket: string
  org: string
  type: DataLoaderType
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class DataLoadersWizard extends PureComponent<Props> {
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
    const {
      currentStepIndex,
      visible,
      bucket,
      org,
      onSetBucketInfo,
      buckets,
      type,
    } = this.props

    return (
      <WizardOverlay
        visible={visible}
        title={'Data Loading'}
        onDismiss={this.handleDismiss}
      >
        <StepSwitcher
          currentStepIndex={currentStepIndex}
          onboardingStepProps={this.stepProps}
          bucketName={bucket}
          onSetBucketInfo={onSetBucketInfo}
          type={type}
          org={org}
          buckets={buckets}
        />
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
    const {startingStep, startingType, startingSubstep} = this.props

    const hasStartingStep = startingStep || startingStep === 0
    const hasStartingSubstep = startingSubstep || startingSubstep === 0
    const hasStartingType =
      startingType || startingType === DataLoaderType.Empty

    if (hasStartingType) {
      this.props.onSetDataLoadersType(startingType)
    }

    if (hasStartingSubstep) {
      this.props.onSetSubstepIndex(
        hasStartingStep ? startingStep : 0,
        startingSubstep
      )
    } else if (hasStartingStep) {
      this.props.onSetCurrentStepIndex(startingStep)
    }
  }

  private handleDismiss = () => {
    this.props.onCompleteSetup()
    this.props.onClearDataLoaders()
    this.props.onClearSteps()
  }

  private get stepProps(): DataLoaderStepProps {
    const {
      links,
      notify,
      substep,
      onCompleteSetup,
      currentStepIndex,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return {
      substep,
      currentStepIndex,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onIncrementCurrentStepIndex,
      onDecrementCurrentStepIndex,
      links,
      notify,
      onCompleteSetup,
      onExit: this.handleDismiss,
    }
  }
}

const mstp = ({
  links,
  dataLoading: {
    dataLoaders: {type},
    steps: {currentStep, substep, bucket, org},
  },
  me: {name},
}: AppState): StateProps => ({
  links,
  type,
  currentStepIndex: currentStep,
  substep,
  username: name,
  bucket,
  org,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetBucketInfo: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onSetSubstepIndex: setSubstepIndex,
  onClearDataLoaders: clearDataLoaders,
  onClearSteps: clearSteps,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DataLoadersWizard)
