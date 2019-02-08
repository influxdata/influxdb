// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
  Button,
  ComponentColor,
  ComponentSpacer,
} from 'src/clockface'
import {Telegraf} from '@influxdata/influx'
import EditableName from 'src/shared/components/EditableName'

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
            <EditableName
              onUpdate={this.handleUpdateConfig}
              name={collector.name}
            />
          </IndexList.Cell>
          <IndexList.Cell>{bucket}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ComponentSpacer align={Alignment.Right}>
              <Button
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Secondary}
                text={'View'}
                onClick={this.handleOpenConfig}
              />
              <Button
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Secondary}
                text={'Setup Details'}
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

  private handleUpdateConfig = (name: string) => {
    const {onUpdate, collector} = this.props
    onUpdate({...collector, name})
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
