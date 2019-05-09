// Libraries
import React, {PureComponent} from 'react'

// Components
import {ComponentSize, Label} from '@influxdata/clockface'
import {IndexList, Alignment, ConfirmationButton} from 'src/clockface'

// Types
import {ILabel} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: ILabel
  onClick: (labelID: string) => void
  onDelete: (labelID: string) => void
}

@ErrorHandling
export default class LabelRow extends PureComponent<Props> {
  public render() {
    const {label, onDelete} = this.props

    return (
      <IndexList.Row key={label.id}>
        <IndexList.Cell>
          <Label
            id={label.id}
            name={label.name}
            color={label.properties.color}
            description={label.properties.description}
            size={ComponentSize.Small}
            onClick={this.handleClick}
          />
        </IndexList.Cell>
        <IndexList.Cell>{label.properties.description}</IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <ConfirmationButton
            text="Delete"
            confirmText="Confirm"
            size={ComponentSize.ExtraSmall}
            onConfirm={onDelete}
            returnValue={label.id}
          />
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleClick = (): void => {
    const {label, onClick} = this.props

    onClick(label.id)
  }
}
