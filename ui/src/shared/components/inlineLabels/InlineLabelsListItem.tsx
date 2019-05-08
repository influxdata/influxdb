// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Components
import {Label} from '@influxdata/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  active: boolean
  id: string
  name: string
  colorHex: string
  description: string
  onClick: (labelID: string) => void
  onMouseOver: (labelID: string) => void
}

@ErrorHandling
class InlineLabelsListItem extends Component<Props> {
  public render() {
    const {name, colorHex, description, id} = this.props

    return (
      <div
        className={this.className}
        onMouseOver={this.handleMouseOver}
        onClick={this.handleClick}
        data-testid={`label-list--item ${name}`}
      >
        <Label name={name} description={description} id={id} color={colorHex} />
      </div>
    )
  }

  private handleMouseOver = (): void => {
    const {onMouseOver, id} = this.props

    onMouseOver(id)
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>): void => {
    e.stopPropagation()
    const {onClick, id} = this.props

    onClick(id)
  }

  private get className(): string {
    const {active} = this.props

    return classnames('inline-labels--list-item', {active})
  }
}

export default InlineLabelsListItem
