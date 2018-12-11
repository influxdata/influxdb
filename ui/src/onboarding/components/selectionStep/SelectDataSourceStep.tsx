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
import DataSourceTypeSelector from 'src/onboarding/components/selectionStep/TypeSelector'
import StreamingDataSourceSelector from 'src/onboarding/components/selectionStep/StreamingSelector'

// Actions
import {setActiveTelegrafPlugin} from 'src/onboarding/actions/dataLoaders'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  ConfigurationState,
  TelegrafPluginName,
} from 'src/types/v2/dataLoaders'

export interface OwnProps extends OnboardingStepProps {
  bucket: string
  telegrafPlugins: TelegrafPlugin[]
  type: DataLoaderType
  onAddTelegrafPlugin: (telegrafPlugin: TelegrafPlugin) => void
  onRemoveTelegrafPlugin: (TelegrafPlugin: string) => void
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
        <StreamingDataSourceSelector
          telegrafPlugins={this.props.telegrafPlugins}
          onToggleTelegrafPlugin={this.handleToggleTelegrafPlugin}
        />
      )
    }
    return (
      <DataSourceTypeSelector
        onSelectTelegrafPlugin={this.handleSelectTelegrafPlugin}
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

  private handleSelectTelegrafPlugin = async (
    telegrafPlugin: DataLoaderType
  ) => {
    await this.props.onSetDataLoadersType(telegrafPlugin)
    this.handleClickNext()

    return
  }

  private handleToggleTelegrafPlugin = (
    telegrafPlugin: TelegrafPluginName,
    isSelected: boolean
  ) => {
    if (isSelected) {
      this.props.onRemoveTelegrafPlugin(telegrafPlugin)

      return
    }

    const plugin: TelegrafPlugin = {
      name: telegrafPlugin,
      configured: ConfigurationState.Unconfigured,
      active: false,
    }
    this.props.onAddTelegrafPlugin(plugin)
  }

  private get isStreaming(): boolean {
    return this.props.params.substepID === 'streaming'
  }
}

export default withRouter<OwnProps>(SelectDataSourceStep)
