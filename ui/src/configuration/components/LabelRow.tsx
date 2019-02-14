// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  Button,
  Alignment,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'
import {IndexList, Label} from 'src/clockface'

// Types
import {LabelType} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: LabelType
}

@ErrorHandling
export default class LabelRow extends PureComponent<Props> {
  public render() {
    const {label} = this.props

    return (
      <IndexList.Row key={label.id}>
        <IndexList.Cell>
          <Label
            id={label.id}
            name={label.name}
            colorHex={label.colorHex}
            description={label.description}
            size={ComponentSize.Small}
            onClick={label.onClick}
          />
        </IndexList.Cell>
        <IndexList.Cell>{label.description}</IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <Button
            text="Delete"
            color={ComponentColor.Danger}
            size={ComponentSize.ExtraSmall}
            onClick={this.handleClick}
          />
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleClick = () => {
    const {label} = this.props

    label.onDelete(label.id)
  }
}
