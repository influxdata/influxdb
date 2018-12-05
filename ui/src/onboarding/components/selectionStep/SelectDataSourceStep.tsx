// Libraries
import React, {PureComponent} from 'react'

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

// Types
import {TelegrafRequestPlugins} from 'src/api'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  ConfigurationState,
} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  bucket: string
  telegrafPlugins: TelegrafPlugin[]
  type: DataLoaderType
  onAddTelegrafPlugin: (telegrafPlugin: TelegrafPlugin) => void
  onRemoveTelegrafPlugin: (TelegrafPlugin: string) => void
  onSetDataLoadersType: (type: DataLoaderType) => void
}

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
    if (this.state.showStreamingSources) {
      return `Select Streaming Data Sources to add to ${bucket ||
        'your bucket'}`
    }
    return `Select a Data Source to add to ${bucket || 'your bucket'}`
  }

  private get selector(): JSX.Element {
    if (
      this.props.type === DataLoaderType.Streaming &&
      this.state.showStreamingSources
    ) {
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
    if (
      this.props.type === DataLoaderType.Streaming &&
      !this.state.showStreamingSources
    ) {
      this.setState({showStreamingSources: true})
      return
    }

    this.props.onIncrementCurrentStepIndex()
  }

  private handleClickBack = () => {
    if (this.props.type === DataLoaderType.Streaming) {
      this.setState({showStreamingSources: false})
      return
    }

    this.props.onDecrementCurrentStepIndex()
  }

  private handleSelectTelegrafPlugin = (telegrafPlugin: DataLoaderType) => {
    this.props.onSetDataLoadersType(telegrafPlugin)
    return
  }

  private handleToggleTelegrafPlugin = (
    telegrafPlugin: TelegrafRequestPlugins.NameEnum,
    isSelected: boolean
  ) => {
    const {telegrafPlugins} = this.props

    if (isSelected) {
      this.props.onRemoveTelegrafPlugin(telegrafPlugin)

      return
    }

    const active = telegrafPlugins.length === 0
    this.props.onAddTelegrafPlugin({
      name: telegrafPlugin,
      configured: ConfigurationState.Unconfigured,
      active,
      config: {},
    })
  }
}

export default SelectDataSourceStep
