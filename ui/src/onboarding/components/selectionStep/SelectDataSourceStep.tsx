// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import TypeSelector from 'src/onboarding/components/selectionStep/TypeSelector'
import StreamingSelector from 'src/onboarding/components/selectionStep/StreamingSelector'

// Actions
import {
  setActiveTelegrafPlugin,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  BundleName,
} from 'src/types/v2/dataLoaders'

export interface OwnProps extends OnboardingStepProps {
  bucket: string
  telegrafPlugins: TelegrafPlugin[]
  pluginBundles: BundleName[]
  type: DataLoaderType
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onSetDataLoadersType: (type: DataLoaderType) => void
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
}

interface RouterProps {
  params: {
    stepID: string
    substepID: string
  }
}

type Props = OwnProps & RouterProps & WithRouterProps

interface State {
  showStreamingSources: boolean
}

@ErrorHandling
class SelectDataSourceStep extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {showStreamingSources: false}
  }

  public render() {
    return (
      <div className="onboarding-step">
        <h3 className="wizard-step--title">{this.title}</h3>
        <h5 className="wizard-step--sub-title">
          You will be able to configure additional Data Sources later
        </h5>
        {this.selector}
        <div className="wizard-button-container">
          <div className="wizard-button-bar">
            <Button
              color={ComponentColor.Default}
              text="Back"
              size={ComponentSize.Medium}
              onClick={this.handleClickBack}
            />
            <Button
              color={ComponentColor.Primary}
              text="Next"
              size={ComponentSize.Medium}
              onClick={this.handleClickNext}
              status={ComponentStatus.Default}
              titleText={'Next'}
            />
          </div>
          {this.skipLink}
        </div>
      </div>
    )
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

  private get skipLink() {
    return (
      <Button
        color={ComponentColor.Default}
        text="Skip"
        size={ComponentSize.Small}
        onClick={this.jumpToCompletionStep}
      >
        skip
      </Button>
    )
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    onSetCurrentStepIndex(stepStatuses.length - 1)
  }

  private handleClickNext = () => {
    const {
      router,
      params: {stepID},
      telegrafPlugins,
      onSetActiveTelegrafPlugin,
    } = this.props

    if (this.props.type === DataLoaderType.Streaming && !this.isStreaming) {
      router.push(`/onboarding/${stepID}/streaming`)
      return
    }

    const name = _.get(telegrafPlugins, '0.name', '')
    onSetActiveTelegrafPlugin(name)

    this.props.onIncrementCurrentStepIndex()
  }

  private handleClickBack = () => {
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
    return this.props.params.substepID === 'streaming'
  }
}

export default withRouter<OwnProps>(SelectDataSourceStep)
