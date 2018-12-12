// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Components
import Label from 'src/clockface/components/label/Label'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  highlighted: boolean
  id: string
  text: string
  colorHex: string
  description: string
  onClick: (labelID: string) => void
  onHighlight: (labelID: string) => void
}

@ErrorHandling
class LabelSelectorMenuItem extends Component<Props> {
  public render() {
    const {text, colorHex, description, id} = this.props

    return (
      <div
        className={this.className}
        onMouseOver={this.handleMouseOver}
        onClick={this.handleClick}
      >
        <div className="label-selector--label">
          <Label
            text={text}
            description={description}
            id={id}
            colorHex={colorHex}
          />
        </div>
        <div className="label-selector--description">{description}</div>
      </div>
    )
  }

  private get className(): string {
    const {highlighted} = this.props

    return classnames('label-selector--menu-item', {active: highlighted})
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
