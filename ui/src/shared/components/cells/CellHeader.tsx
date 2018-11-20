// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  name: string
  isEditable: boolean
}

@ErrorHandling
class CellHeader extends PureComponent<Props> {
  public render() {
    const {isEditable, name} = this.props

    if (isEditable) {
      return (
        <div className="cell--header cell--draggable">
          <label className={this.cellNameClass}>{name}</label>
          <div className="cell--header-bar" />
        </div>
      )
    }

    return (
      <div className="cell--header">
        <label className="cell--name">{name}</label>
      </div>
    )
  }

  private get cellNameClass(): string {
    const {name} = this.props

    const isNameBlank = !!name.trim()

    return classnames('cell--name', {'cell--name__blank': isNameBlank})
  }
}

export default CellHeader
