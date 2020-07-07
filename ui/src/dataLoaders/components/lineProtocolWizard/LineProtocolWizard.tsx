// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {Overlay} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
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
import {AppState, ResourceType} from 'src/types'
import {Bucket} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors'

export interface LineProtocolStepProps {
  currentStepIndex: number
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  notify: typeof notifyAction
  onExit: () => void
}

interface OwnProps {
  onCompleteSetup: () => void
  startingStep?: number
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

@ErrorHandling
class LineProtocolWizard extends PureComponent<
  Props & RouteComponentProps<{orgID: string}>
> {
  public componentDidMount() {
    this.handleSetBucketInfo()
    this.handleSetStartingValues()
  }

  public render() {
    const {buckets} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Add Data Using Line Protocol"
            onDismiss={this.handleDismiss}
          />
          <LineProtocolStepSwitcher
            stepProps={this.stepProps}
            buckets={buckets}
          />
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleSetBucketInfo = () => {
    const {bucket, buckets} = this.props
    if (!bucket && buckets && buckets.length) {
      const {orgID, name, id} = buckets[0]

      this.props.onSetBucketInfo(orgID, name, id)
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
    const {history, onClearDataLoaders, onClearSteps} = this.props

    onClearDataLoaders()
    onClearSteps()
    history.goBack()
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

const mstp = (state: AppState) => {
  const {
    dataLoading: {
      steps: {currentStep, bucket},
    },
    me: {name},
  } = state

  const buckets = getAll<Bucket>(state, ResourceType.Buckets)

  return {
    currentStepIndex: currentStep,
    username: name,
    bucket,
    buckets,
  }
}

const mdtp = {
  notify: notifyAction,
  onSetBucketInfo: setBucketInfo,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onClearDataLoaders: clearDataLoaders,
  onClearSteps: clearSteps,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(LineProtocolWizard))
