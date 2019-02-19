// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import Label from 'src/clockface/components/label/Label'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  highlighted: boolean
  id: string
  name: string
  colorHex: string
  description: string
  onClick: (labelID: string) => void
  onHighlight: (labelID: string) => void
}

@ErrorHandling
class LabelSelectorMenuItem extends Component<Props> {
  public render() {
    const {name, colorHex, description, id} = this.props

    return (
      <span
        className="label-selector--menu-item"
        onMouseOver={this.handleMouseOver}
        onClick={this.handleClick}
      >
        <Label
          name={name}
          description={description}
          id={id}
          colorHex={colorHex}
        />
      </span>
    )
  }

  private handleMouseOver = (): void => {
    const {onHighlight, id} = this.props

    onHighlight(id)
  }

  private handleClick = (): void => {
    const {onClick, id} = this.props

    onClick(id)
  }
}

export default LabelSelectorMenuItem
