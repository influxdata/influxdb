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
  name: string
  colorHex: string
  description: string
  onClick: (labelID: string) => void
  onHighlight: (labelID: string) => void
}

@ErrorHandling
class InlineLabelEditorMenuItem extends Component<Props> {
  public render() {
    const {name, colorHex, description, id} = this.props

    return (
      <div
        className={this.className}
        onMouseOver={this.handleMouseOver}
        onClick={this.handleClick}
      >
        <Label
          name={name}
          description={description}
          id={id}
          colorHex={colorHex}
        />
      </div>
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

  private get className(): string {
    const {highlighted} = this.props

    return classnames('inline-label-editor--menu-item', {active: highlighted})
  }
}

export default InlineLabelEditorMenuItem
