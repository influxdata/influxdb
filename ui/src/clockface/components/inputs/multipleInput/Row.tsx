// Libraries
import React, {PureComponent} from 'react'
import uuid from 'uuid'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import {
  IndexList,
  ComponentSize,
  Alignment,
  ConfirmationButton,
  ComponentColor,
  Appearance,
} from '@influxdata/clockface'
import EditableDescription from 'src/shared/components/editable_description/EditableDescription'

interface Item {
  text?: string
  name?: string
}

interface RowProps {
  confirmText?: string
  item: Item
  onDelete: (item: Item) => void
  fieldName: string
  index: number
  onChange: (index: number, value: string) => void
}

@ErrorHandling
class Row extends PureComponent<RowProps> {
  public static defaultProps: Partial<RowProps> = {
    confirmText: 'Delete',
  }

  public render() {
    const {item, fieldName} = this.props
    return (
      <IndexList>
        <IndexList.Body emptyState={<div />} columnCount={2}>
          <IndexList.Row key={uuid.v4()} disabled={false}>
            <IndexList.Cell>
              <EditableDescription
                description={item.text}
                placeholder={`Edit ${fieldName}`}
                onUpdate={this.handleKeyDown}
              />
            </IndexList.Cell>
            <IndexList.Cell alignment={Alignment.Right}>
              <ConfirmationButton
                onConfirm={this.handleClickDelete}
                returnValue={item}
                text="Delete"
                popoverColor={ComponentColor.Danger}
                popoverAppearance={Appearance.Outline}
                color={ComponentColor.Danger}
                confirmationLabel=""
                confirmationButtonColor={ComponentColor.Danger}
                confirmationButtonText="Confirm"
                size={ComponentSize.ExtraSmall}
              />
            </IndexList.Cell>
          </IndexList.Row>
        </IndexList.Body>
      </IndexList>
    )
  }

  private handleClickDelete = (item: Item) => {
    this.props.onDelete(item)
  }

  private handleKeyDown = (value: string) => {
    const {onChange, index} = this.props

    onChange(index, value)
  }
}

export default Row
