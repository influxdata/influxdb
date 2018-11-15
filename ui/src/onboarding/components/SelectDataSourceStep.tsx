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
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import GridSizer from 'src/clockface/components/grid_sizer/GridSizer'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {DataSource, ConfigurationState} from 'src/types/v2/dataSources'

const DATA_SOURCE_OPTIONS = ['System', 'CSV Data', 'Line Protocol', 'Redis']

export interface Props extends OnboardingStepProps {
  bucket: string
  dataSources: DataSource[]
  onAddDataSource: (dataSource: DataSource) => void
  onRemoveDataSource: (dataSource: string) => void
}

@ErrorHandling
class SelectDataSourceStep extends PureComponent<Props> {
  public render() {
    return (
      <div className="onboarding-step">
        <h3 className="wizard-step--title">{this.title}</h3>
        <h5 className="wizard-step--sub-title">
          You can configure additional Data Sources later
        </h5>
        <GridSizer>
          {DATA_SOURCE_OPTIONS.map(ds => {
            return (
              <CardSelectCard
                key={ds}
                id={ds}
                name={ds}
                label={ds}
                checked={this.isCardChecked(ds)}
                onClick={this.toggleChecked(ds)}
              />
            )
          })}
        </GridSizer>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={this.props.onDecrementCurrentStepIndex}
          />
          <Button
            color={ComponentColor.Primary}
            text="Next"
            size={ComponentSize.Medium}
            onClick={this.props.onIncrementCurrentStepIndex}
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

  private isCardChecked(dataSource: string) {
    const {dataSources} = this.props

    if (dataSources.find(ds => ds.name === dataSource)) {
      return true
    }
    return false
  }

  private toggleChecked = (dataSource: string) => () => {
    if (this.isCardChecked(dataSource)) {
      this.props.onRemoveDataSource(dataSource)

      return
    }

    this.props.onAddDataSource({
      name: dataSource,
      configured: ConfigurationState.Unconfigured,
      configs: null,
    })
  }
}

export default SelectDataSourceStep
