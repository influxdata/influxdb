// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

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

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  BundleName,
} from 'src/types/v2/dataLoaders'

export interface OwnProps extends DataLoaderStepProps {
  bucket: string
  telegrafPlugins: TelegrafPlugin[]
  pluginBundles: BundleName[]
  type: DataLoaderType
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onSetDataLoadersType: (type: DataLoaderType) => void
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetStepStatus: (index: number, status: StepStatus) => void
}

type Props = OwnProps

interface State {
  showStreamingSources: boolean
}

@ErrorHandling
export class SelectDataSourceStep extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {showStreamingSources: false}
  }

  public componentDidMount() {
    if (this.isStreaming && this.props.type !== DataLoaderType.Streaming) {
      this.props.onSetDataLoadersType(DataLoaderType.Streaming)
    }
  }

  public render() {
    return (
      <div className={this.skippableClassName}>
        <Form onSubmit={this.handleClickNext}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <h3 className="wizard-step--title">{this.title}</h3>
                <h5 className="wizard-step--sub-title">
                  You will be able to configure additional Data Sources later
                </h5>
                {this.selector}
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={this.handleClickBack}
            onClickSkip={this.jumpToCompletionStep}
            skipButtonText={'Skip to Complete'}
            autoFocusNext={true}
            nextButtonStatus={this.nextButtonStatus}
            showSkip={this.showSkip}
          />
        </Form>
      </div>
    )
  }

  private get nextButtonStatus(): ComponentStatus {
    const {type, telegrafPlugins} = this.props

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
      return `Select Streaming Data Sources to add to ${bucket ||
        'your bucket'}`
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

  private get showSkip(): boolean {
    const {telegrafPlugins} = this.props
    if (telegrafPlugins.length < 1) {
      return false
    }

    return telegrafPlugins.every(plugin => plugin.configured === 'configured')
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    this.handleSetStepStatus()
    onSetCurrentStepIndex(stepStatuses.length - 2)
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

    this.handleSetStepStatus()

    if (this.isStreaming) {
      const name = _.get(telegrafPlugins, '0.name', '')
      onSetActiveTelegrafPlugin(name)
      onSetSubstepIndex(currentStepIndex + 1, 0)
      return
    }

    this.props.onIncrementCurrentStepIndex()
  }

  private handleClickBack = () => {
    const {currentStepIndex, onSetCurrentStepIndex} = this.props

    if (this.isStreaming) {
      onSetCurrentStepIndex(+currentStepIndex)
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

  private handleSetStepStatus = () => {
    const {onSetStepStatus, currentStepIndex} = this.props

    if (
      this.props.type === DataLoaderType.Streaming &&
      !this.props.telegrafPlugins.length
    ) {
      onSetStepStatus(currentStepIndex, StepStatus.Incomplete)
    } else if (this.props.type) {
      onSetStepStatus(currentStepIndex, StepStatus.Complete)
    }
  }

  private get isStreaming(): boolean {
    return this.props.substep === 'streaming'
  }

  private get skippableClassName(): string {
    const {telegrafPlugins} = this.props
    const pluginsSelected = telegrafPlugins.length > 0
    const allConfigured = telegrafPlugins.every(
      plugin => plugin.configured === 'configured'
    )

    return classnames('onboarding-step', {
      'wizard--skippable': pluginsSelected && allConfigured,
    })
  }
}

export default SelectDataSourceStep
