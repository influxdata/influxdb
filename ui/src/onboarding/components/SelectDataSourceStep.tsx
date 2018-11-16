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
import DataSourceSelector from 'src/onboarding/components/DataSourceSelector'
import StreamingDataSourceSelector from 'src/onboarding/components/StreamingDataSourcesSelector'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {DataSource, ConfigurationState} from 'src/types/v2/dataSources'

export interface Props extends OnboardingStepProps {
  bucket: string
  dataSources: DataSource[]
  onAddDataSource: (dataSource: DataSource) => void
  onRemoveDataSource: (dataSource: string) => void
  onSetDataSources: (dataSources: DataSource[]) => void
}

export enum StreamingOptions {
  NotSelected = 'not selected',
  Selected = 'selected',
  Show = 'show',
}

interface State {
  streaming: StreamingOptions
}

@ErrorHandling
class SelectDataSourceStep extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {streaming: StreamingOptions.NotSelected}
  }

  public render() {
    return (
      <div className="onboarding-step">
        <h3 className="wizard-step--title">{this.title}</h3>
        <h5 className="wizard-step--sub-title">
          You can configure additional Data Sources later
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

    return `Select Data Source for Bucket ${bucket || ''}`
  }

  private get selector(): JSX.Element {
    if (this.state.streaming === StreamingOptions.Show) {
      return (
        <StreamingDataSourceSelector
          dataSources={this.props.dataSources}
          onToggleDataSource={this.handleToggleDataSource}
        />
      )
    }
    return (
      <DataSourceSelector
        dataSources={this.props.dataSources}
        onSelectDataSource={this.handleSelectDataSource}
        streaming={this.state.streaming}
      />
    )
  }

  private handleClickNext = () => {
    if (this.state.streaming === StreamingOptions.Selected) {
      this.setState({streaming: StreamingOptions.Show})
      return
    }

    this.props.onIncrementCurrentStepIndex()
  }

  private handleClickBack = () => {
    if (this.state.streaming === StreamingOptions.Show) {
      this.setState({streaming: StreamingOptions.NotSelected})
      return
    }

    this.props.onDecrementCurrentStepIndex()
  }

  private handleSelectDataSource = (dataSource: string) => {
    switch (dataSource) {
      case 'Streaming':
        this.setState({streaming: StreamingOptions.Selected})
        this.props.onSetDataSources([])
        break
      default:
        this.setState({streaming: StreamingOptions.NotSelected})
        this.props.onSetDataSources([
          {
            name: dataSource,
            configured: ConfigurationState.Unconfigured,
            active: true,
            configs: null,
          },
        ])
        break
    }
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
