// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  active: boolean
  id: string
  name: string
  onClick: () => void
  onMouseOver: (labelID: string) => void
}

@ErrorHandling
class InlineLabelsCreateLabelButton extends Component<Props> {
  public render() {
    const {name} = this.props

    return (
      <div
        className={this.className}
        onMouseOver={this.handleMouseOver}
        onClick={this.handleClick}
        data-testid="inline-labels--create-new"
      >
        Create "<strong>{`${name}`}</strong>"
      </div>
    )
  }

  private handleMouseOver = (): void => {
    const {onMouseOver, id} = this.props

    onMouseOver(id)
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>): void => {
    e.stopPropagation()
    this.props.onClick()
  }

  private get className(): string {
    const {active} = this.props

    return classnames('inline-labels--list-item inline-labels--create-new', {
      active,
    })
  }
}

export default InlineLabelsCreateLabelButton
