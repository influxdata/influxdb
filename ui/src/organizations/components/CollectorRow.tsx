// Libraries
import React, {PureComponent} from 'react'

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

// Constants
import {DEFAULT_COLLECTOR_NAME} from 'src/dashboards/constants'

interface Props {
  collector: Telegraf
  bucket: string
  onDelete: (telegrafID: string) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
  onOpenTelegrafConfig: (telegrafID: string, telegrafName: string) => void
}

export default class CollectorRow extends PureComponent<Props> {
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
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Secondary}
                text={'Setup Instructions'}
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

  private handleUpdateName = (name: string) => {
    const {onUpdate, collector} = this.props

    onUpdate({...collector, name})
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
    this.props.onDelete(this.props.collector.id)
  }

  private handleOpenInstructions = (): void => {
    this.props.onOpenInstructions(this.props.collector.id)
  }
}
