// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import CollectorsStepSwitcher from 'src/dataLoaders/components/collectorsWizard/CollectorsStepSwitcher'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  setBucketInfo,
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
  setCurrentStepIndex,
  clearSteps,
} from 'src/dataLoaders/actions/steps'

import {
  clearDataLoaders,
  setActiveTelegrafPlugin,
  setPluginConfiguration,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {Links} from 'src/types/links'
import {Substep, TelegrafPlugin} from 'src/types/dataLoaders'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types'
import {Bucket} from '@influxdata/influx'

export interface CollectorsStepProps {
  currentStepIndex: number
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  notify: (message: Notification | NotificationFunc) => void
  onExit: () => void
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetBucketInfo: typeof setBucketInfo
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onClearDataLoaders: typeof clearDataLoaders
  onClearSteps: typeof clearSteps
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
}

interface StateProps {
  links: Links
  buckets: Bucket[]
  telegrafPlugins: TelegrafPlugin[]
  currentStepIndex: number
  substep: Substep
  username: string
  bucket: string
}

interface State {
  buckets: Bucket[]
}

type Props = StateProps & DispatchProps

@ErrorHandling
class CollectorsWizard extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      buckets: [],
    }
  }
  public componentDidMount() {
    this.handleSetBucketInfo()
    this.props.onSetCurrentStepIndex(0)
  }

  public render() {
    const {buckets} = this.props

    return (
      <WizardOverlay
        title="Create a Telegraf Config"
        onDismiss={this.handleDismiss}
      >
        <CollectorsStepSwitcher stepProps={this.stepProps} buckets={buckets} />
      </WizardOverlay>
    )
  }

  private handleSetBucketInfo = () => {
    const {bucket, buckets} = this.props
    if (!bucket && (buckets && buckets.length)) {
      const {orgID, name, id} = buckets[0]

      this.props.onSetBucketInfo(orgID, name, id)
    }
  }

  private handleDismiss = () => {
    const {router, onClearDataLoaders, onClearSteps} = this.props

    onClearDataLoaders()
    onClearSteps()
    router.goBack()
  }

  private get stepProps(): CollectorsStepProps {
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
  links,
  buckets,
  dataLoading: {
    dataLoaders: {telegrafPlugins},
    steps: {currentStep, substep, bucket},
  },
  me: {name},
}: AppState): StateProps => ({
  links,
  telegrafPlugins,
  currentStepIndex: currentStep,
  substep,
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
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(CollectorsWizard))
