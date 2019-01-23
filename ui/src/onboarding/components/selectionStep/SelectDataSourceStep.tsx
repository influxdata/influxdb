// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {ComponentStatus, Form} from 'src/clockface'
import TypeSelector from 'src/onboarding/components/selectionStep/TypeSelector'
import StreamingSelector from 'src/onboarding/components/selectionStep/StreamingSelector'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  setActiveTelegrafPlugin,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
} from 'src/onboarding/actions/dataLoaders'
import {setBucketInfo} from 'src/onboarding/actions/steps'

// Types
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  BundleName,
} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

export interface Props extends DataLoaderStepProps {
  bucket: string
  telegrafPlugins: TelegrafPlugin[]
  pluginBundles: BundleName[]
  type: DataLoaderType
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onSetDataLoadersType: (type: DataLoaderType) => void
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetBucketInfo: typeof setBucketInfo
  buckets: Bucket[]
  selectedBucket: string
}

@ErrorHandling
export class SelectDataSourceStep extends PureComponent<Props> {
  public componentDidMount() {
    if (this.isStreaming && this.props.type !== DataLoaderType.Streaming) {
      this.props.onSetDataLoadersType(DataLoaderType.Streaming)
    }
  }

  public render() {
    return (
      <div className="onboarding-step">
        <Form onSubmit={this.handleClickNext}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <h3 className="wizard-step--title">{this.title}</h3>
                <h5 className="wizard-step--sub-title">
                  Telegraf collects and writes metrics to a bucket in InfluxDB.
                </h5>
                {this.selector}
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={this.handleClickBack}
            autoFocusNext={true}
            nextButtonStatus={this.nextButtonStatus}
          />
        </Form>
      </div>
    )
  }

  private get nextButtonStatus(): ComponentStatus {
    const {type, telegrafPlugins, buckets} = this.props

    if (!buckets || !buckets.length) {
      return ComponentStatus.Disabled
    }

    const isTypeEmpty = type === DataLoaderType.Empty
    const isStreamingWithoutPlugin =
      type === DataLoaderType.Streaming &&
      this.isStreaming &&
      !telegrafPlugins.length

    if (isTypeEmpty || isStreamingWithoutPlugin) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private get title(): string {
    const {bucket} = this.props
    if (this.isStreaming) {
      return `Select Telegraf Plugins to add to ${bucket || 'your bucket'}`
    }
    return `Select a Data Source to add to ${bucket || 'your bucket'}`
  }

  private get selector(): JSX.Element {
    if (this.props.type === DataLoaderType.Streaming && this.isStreaming) {
      return (
        <StreamingSelector
          pluginBundles={this.props.pluginBundles}
          telegrafPlugins={this.props.telegrafPlugins}
          onTogglePluginBundle={this.handleTogglePluginBundle}
          buckets={this.props.buckets}
          bucket={this.props.bucket}
          selectedBucket={this.props.selectedBucket}
          onSelectBucket={this.handleSelectBucket}
        />
      )
    }
    return (
      <TypeSelector
        onSelectDataLoaderType={this.handleSelectDataLoaderType}
        type={this.props.type}
      />
    )
  }

  private handleSelectBucket = (bucket: Bucket) => {
    const {organization, organizationID, id, name} = bucket

    this.props.onSetBucketInfo(organization, organizationID, name, id)
  }

  private handleClickNext = () => {
    const {
      currentStepIndex,
      telegrafPlugins,
      onSetActiveTelegrafPlugin,
      onSetSubstepIndex,
    } = this.props

    if (this.props.type === DataLoaderType.Streaming && !this.isStreaming) {
      onSetSubstepIndex(currentStepIndex, 'streaming')
      onSetActiveTelegrafPlugin('')
      return
    }

    if (this.isStreaming) {
      const name = _.get(telegrafPlugins, '0.name', '')
      onSetActiveTelegrafPlugin(name)
      onSetSubstepIndex(currentStepIndex + 1, 0)
      return
    }

    this.props.onIncrementCurrentStepIndex()
  }

  private handleClickBack = () => {
    const {currentStepIndex, onSetSubstepIndex} = this.props

    if (this.isStreaming) {
      onSetSubstepIndex(+currentStepIndex, 0)
      return
    }

    this.props.onDecrementCurrentStepIndex()
  }

  private handleSelectDataLoaderType = async (type: DataLoaderType) => {
    await this.props.onSetDataLoadersType(type)
    this.handleClickNext()

    return
  }

  private handleTogglePluginBundle = (
    bundle: BundleName,
    isSelected: boolean
  ) => {
    if (isSelected) {
      this.props.onRemovePluginBundle(bundle)

      return
    }

    this.props.onAddPluginBundle(bundle)
  }

  private get isStreaming(): boolean {
    return this.props.substep === 'streaming'
  }
}

export default SelectDataSourceStep
