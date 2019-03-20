// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  ComponentSpacer,
  Stack,
  IndexList,
  ConfirmationButton,
} from 'src/clockface'
import {
  ComponentSize,
  Alignment,
  Button,
  ComponentColor,
} from '@influxdata/clockface'
import {Telegraf} from '@influxdata/influx'
import EditableName from 'src/shared/components/EditableName'
import EditableDescription from 'src/shared/components/editable_description/EditableDescription'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Constants
import {DEFAULT_COLLECTOR_NAME} from 'src/dashboards/constants'

// Types
import {AppState} from 'src/types/v2'
import {ILabel} from '@influxdata/influx'

interface OwnProps {
  collector: Telegraf
  bucket: string
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
  onOpenTelegrafConfig: (telegrafID: string, telegrafName: string) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: ILabel[]
}

type Props = OwnProps & StateProps

class CollectorRow extends PureComponent<Props> {
  public render() {
    const {collector, bucket} = this.props

    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <ComponentSpacer
              stackChildren={Stack.Rows}
              align={Alignment.Left}
              stretchToFitWidth={true}
            >
              <EditableName
                onUpdate={this.handleUpdateName}
                name={collector.name}
                noNameString={DEFAULT_COLLECTOR_NAME}
                onEditName={this.handleOpenConfig}
              />
              <EditableDescription
                description={collector.description}
                placeholder={`Describe ${collector.name}`}
                onUpdate={this.handleUpdateDescription}
              />
            </ComponentSpacer>
          </IndexList.Cell>
          <IndexList.Cell>{bucket}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ComponentSpacer align={Alignment.Right}>
              <Button
                text="Setup Instructions"
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Secondary}
                onClick={this.handleOpenInstructions}
              />
              <ConfirmationButton
                size={ComponentSize.ExtraSmall}
                text="Delete"
                confirmText="Confirm"
                onConfirm={this.handleDeleteConfig}
              />
            </ComponentSpacer>
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleUpdateName = async (name: string) => {
    const {onUpdate, collector} = this.props

    await onUpdate({...collector, name})
  }

  private handleUpdateDescription = (description: string) => {
    const {onUpdate, collector} = this.props

    onUpdate({...collector, description})
  }

  private handleOpenConfig = (): void => {
    this.props.onOpenTelegrafConfig(
      this.props.collector.id,
      this.props.collector.name
    )
  }

  private handleDeleteConfig = (): void => {
    this.props.onDelete(this.props.collector)
  }

  private handleOpenInstructions = (): void => {
    this.props.onOpenInstructions(this.props.collector.id)
  }
}

const mstp = ({labels}: AppState): StateProps => {
  return {labels: viewableLabels(labels.list)}
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(CollectorRow)
