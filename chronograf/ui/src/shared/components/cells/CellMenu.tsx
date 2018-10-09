// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import MenuTooltipButton, {
  MenuItem,
} from 'src/shared/components/MenuTooltipButton'

// Types
import {Cell, DashboardQuery} from 'src/types/v2/dashboards'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  cell: Cell
  isEditable: boolean
  dataExists: boolean
  onEdit: () => void
  onClone: (cell: Cell) => void
  onDelete: (cell: Cell) => void
  onCSVDownload: () => void
  queries: DashboardQuery[]
}

interface State {
  subMenuIsOpen: boolean
}

@ErrorHandling
class CellMenu extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      subMenuIsOpen: false,
    }
  }

  public render() {
    return <div className={this.contextMenuClassname}>{this.renderMenu}</div>
  }

  private get renderMenu(): JSX.Element {
    const {isEditable} = this.props

    if (isEditable) {
      return (
        <div className="dash-graph-context--buttons">
          {this.pencilMenu}
          <MenuTooltipButton
            icon="duplicate"
            menuItems={this.cloneMenuItems}
            informParent={this.handleToggleSubMenu}
          />
          <MenuTooltipButton
            icon="trash"
            theme="danger"
            menuItems={this.deleteMenuItems}
            informParent={this.handleToggleSubMenu}
          />
        </div>
      )
    }
  }

  private get pencilMenu(): JSX.Element {
    const {dataExists, onCSVDownload, onEdit} = this.props

    const items = [
      {
        text: 'Configure',
        action: onEdit,
        disabled: false,
      },
      {
        text: 'Download CSV',
        action: onCSVDownload,
        disabled: !dataExists,
      },
    ]

    return (
      <MenuTooltipButton
        icon="pencil"
        menuItems={items}
        informParent={this.handleToggleSubMenu}
      />
    )
  }

  private get contextMenuClassname(): string {
    const {subMenuIsOpen} = this.state

    return classnames('dash-graph-context', {
      'dash-graph-context__open': subMenuIsOpen,
    })
  }

  private get cloneMenuItems(): MenuItem[] {
    return [{text: 'Clone Cell', action: this.handleCloneCell, disabled: false}]
  }

  private get deleteMenuItems(): MenuItem[] {
    return [{text: 'Confirm', action: this.handleDeleteCell, disabled: false}]
  }

  private handleDeleteCell = (): void => {
    const {onDelete, cell} = this.props
    onDelete(cell)
  }

  private handleCloneCell = (): void => {
    const {onClone, cell} = this.props
    onClone(cell)
  }

  private handleToggleSubMenu = (): void => {
    this.setState({subMenuIsOpen: !this.state.subMenuIsOpen})
  }
}

export default CellMenu
