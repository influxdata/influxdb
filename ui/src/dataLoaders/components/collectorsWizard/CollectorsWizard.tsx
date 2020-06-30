// Libraries
import React, {PureComponent} from 'react'
import Loadable from 'react-loadable'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Overlay} from '@influxdata/clockface'
import TelegrafEditorFooter from 'src/dataLoaders/components/TelegrafEditorFooter'

const spinner = <div />
const TelegrafEditor = Loadable({
  loader: () => import('src/dataLoaders/components/TelegrafEditor'),
  loading() {
    return spinner
  },
})
const CollectorsStepSwitcher = Loadable({
  loader: () =>
    import(
      'src/dataLoaders/components/collectorsWizard/CollectorsStepSwitcher'
    ),
  loading() {
    return spinner
  },
})
import {isFlagEnabled, FeatureFlag} from 'src/shared/utils/featureFlag'

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
import {AppState, Bucket, Organization, ResourceType} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

// Utils
import {isSystemBucket} from 'src/buckets/constants'

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

type Props = StateProps & DispatchProps
type AllProps = Props & WithRouterProps

@ErrorHandling
class CollectorsWizard extends PureComponent<AllProps> {
  public componentDidMount() {
    const {bucket, buckets} = this.props
    if (!bucket && buckets && buckets.length) {
      const {orgID, name, id} = buckets[0]
      this.props.onSetBucketInfo(orgID, name, id)
    }
    this.props.onSetCurrentStepIndex(0)
  }

  public render() {
    const {buckets} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={1200}>
          <Overlay.Header
            title="Create a Telegraf Configuration"
            onDismiss={this.handleDismiss}
          />
          <Overlay.Body className="data-loading--overlay">
            <FeatureFlag name="telegrafEditor">
              <TelegrafEditor />
            </FeatureFlag>
            <FeatureFlag name="telegrafEditor" equals={false}>
              <CollectorsStepSwitcher
                stepProps={this.stepProps}
                buckets={buckets}
              />
            </FeatureFlag>
          </Overlay.Body>
          <Overlay.Footer>
            <TelegrafEditorFooter onDismiss={this.handleDismiss} />
          </Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
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

const mstp = (state: AppState): StateProps => {
  const {
    links,
    dataLoading: {
      dataLoaders: {telegrafPlugins},
      steps: {currentStep, substep, bucket},
    },
    me: {name},
    telegrafEditor,
  } = state

  const buckets = getAll<Bucket>(state, ResourceType.Buckets)

  const nonSystemBuckets = buckets.filter(
    bucket => !isSystemBucket(bucket.name)
  )

  const org = getOrg(state)

  return {
    links,
    telegrafPlugins,
    text: telegrafEditor.text,
    currentStepIndex: currentStep,
    substep,
    username: name,
    bucket,
    buckets: nonSystemBuckets,
    org,
  }
}

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
