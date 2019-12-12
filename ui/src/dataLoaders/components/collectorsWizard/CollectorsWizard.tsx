// Libraries
import React, {PureComponent} from 'react'
import Loadable from 'react-loadable'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'

const spinner = <div />
const TelegrafEditor = Loadable({
  loader: () => import('src/dataLoaders/components/TelegrafEditor'),
  loading() {
    return spinner
  },
})
const CollectorsStepSwitcher = Loadable({
  loader: () =>
    import('src/dataLoaders/components/collectorsWizard/CollectorsStepSwitcher'),
  loading() {
    return spinner
  },
})
import {isFlagEnabled, FeatureFlag} from 'src/shared/utils/featureFlag'
import {ComponentColor, Button} from '@influxdata/clockface'

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
import {reset} from 'src/dataLoaders/actions/telegrafEditor'

// Types
import {Links} from 'src/types/links'
import {Substep, TelegrafPlugin} from 'src/types/dataLoaders'
import {AppState, Bucket, Organization} from 'src/types'
import {downloadTextFile} from 'src/shared/utils/download'

export interface CollectorsStepProps {
  currentStepIndex: number
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  notify: typeof notifyAction
  onExit: () => void
}

interface DispatchProps {
  notify: typeof notifyAction
  onSetBucketInfo: typeof setBucketInfo
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onClearDataLoaders: typeof clearDataLoaders
  onClearSteps: typeof clearSteps
  onClearTelegrafEditor: typeof reset
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
  text: string
  org: Organization
}

interface State {
  buckets: Bucket[]
}

type Props = StateProps & DispatchProps
type AllProps = Props & WithRouterProps

@ErrorHandling
class CollectorsWizard extends PureComponent<AllProps, State> {
  constructor(props: AllProps) {
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
        footer={
          <FeatureFlag name="telegrafEditor">
            <Button
              color={ComponentColor.Secondary}
              text="Download Config"
              onClick={this.handleDownloadConfig}
            />
            <Button
              color={ComponentColor.Primary}
              text="Save Config"
              onClick={this.handleSaveConfig}
            />
          </FeatureFlag>
        }
      >
        <FeatureFlag name="telegrafEditor">
          <TelegrafEditor />
        </FeatureFlag>
        <FeatureFlag name="telegrafEditor" equals={false}>
          <CollectorsStepSwitcher
            stepProps={this.stepProps}
            buckets={buckets}
          />
        </FeatureFlag>
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

  private handleDownloadConfig = () => {
    downloadTextFile(this.props.text, 'telegraf', '.conf')
  }

  private handleSaveConfig = () => {
    this.handleDismiss()
  }

  private handleDismiss = () => {
    const {router, org} = this.props

    if (isFlagEnabled('telegrafEditor')) {
      const {onClearTelegrafEditor} = this.props
      onClearTelegrafEditor()
    } else {
      const {onClearDataLoaders, onClearSteps} = this.props
      onClearDataLoaders()
      onClearSteps()
    }
    router.push(`/orgs/${org.id}/load-data/telegrafs`)
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
  orgs: {org},
  telegrafEditor,
}: AppState): StateProps => ({
  links,
  telegrafPlugins,
  text: telegrafEditor.text,
  currentStepIndex: currentStep,
  substep,
  username: name,
  bucket,
  buckets: buckets.list,
  org: org,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetBucketInfo: setBucketInfo,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onClearDataLoaders: clearDataLoaders,
  onClearSteps: clearSteps,
  onClearTelegrafEditor: reset,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(CollectorsWizard))
