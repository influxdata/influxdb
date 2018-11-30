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
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {
  DataSource,
  DataLoaderType,
  ConfigurationState,
} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  bucket: string
  dataSources: DataSource[]
  type: DataLoaderType
  onAddDataSource: (dataSource: DataSource) => void
  onRemoveDataSource: (dataSource: string) => void
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
          dataSources={this.props.dataSources}
          onToggleDataSource={this.handleToggleDataSource}
        />
      )
    }
    return (
      <DataSourceTypeSelector
        onSelectDataSource={this.handleSelectDataSource}
        type={this.props.type}
      />
    )
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

  private handleSelectDataSource = (dataSource: DataLoaderType) => {
    this.props.onSetDataLoadersType(dataSource)
    return
  }

  private handleToggleDataSource = (
    dataSource: string,
    isSelected: boolean
  ) => {
    const {dataSources} = this.props

    if (isSelected) {
      this.props.onRemoveDataSource(dataSource)

      return
    }

    const active = dataSources.length === 0
    this.props.onAddDataSource({
      name: dataSource,
      configured: ConfigurationState.Unconfigured,
      active,
      configs: null,
    })
  }
}

export default SelectDataSourceStep
