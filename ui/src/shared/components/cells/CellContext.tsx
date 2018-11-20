// Libraries
import React, {PureComponent} from 'react'

// Components
import {Context, IconFont} from 'src/clockface'

// Types
import {Cell} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  visible: boolean
  cell: Cell
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onCSVDownload: () => void
  onEditCell: () => void
}

@ErrorHandling
class CellContext extends PureComponent<Props> {
  public render() {
    const {onEditCell, onCSVDownload, visible} = this.props

    if (visible) {
      return (
        <div className="cell--context">
          <Context>
            <Context.Menu icon={IconFont.Pencil}>
              <Context.Item label="Configure" action={onEditCell} />
              <Context.Item
                label="Download CSV"
                action={onCSVDownload}
                disabled={true}
              />
            </Context.Menu>
            <Context.Menu icon={IconFont.Duplicate}>
              <Context.Item label="Clone" action={this.handleCloneCell} />
            </Context.Menu>
            <Context.Menu icon={IconFont.Trash}>
              <Context.Item label="Delete" action={this.handleDeleteCell} />
            </Context.Menu>
          </Context>
        </div>
      )
    }

    return null
  }

  private handleDeleteCell = () => {
    const {cell, onDeleteCell} = this.props

    onDeleteCell(cell)
  }

  private handleCloneCell = () => {
    const {cell, onCloneCell} = this.props

    onCloneCell(cell)
  }
}

export default CellContext
