import React, {Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import MenuTooltipButton, {
  MenuItem,
} from 'src/shared/components/MenuTooltipButton'
import CustomTimeIndicator from 'src/shared/components/CustomTimeIndicator'
import {EDITING} from 'src/shared/annotations/helpers'
import {cellSupportsAnnotations} from 'src/shared/constants/index'
import {Cell} from 'src/types/dashboards'
import {QueryConfig} from 'src/types/queries'

import {
  addingAnnotation,
  editingAnnotation,
  dismissEditingAnnotation,
} from 'src/shared/actions/annotations'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Query {
  text: string
  config: QueryConfig
}

interface Props {
  cell: Cell
  isEditable: boolean
  dataExists: boolean
  mode: string
  onEdit: () => void
  onClone: (cell: Cell) => void
  onDelete: (cell: Cell) => void
  onCSVDownload: () => void
  onStartAddingAnnotation: () => void
  onStartEditingAnnotation: () => void
  onDismissEditingAnnotation: () => void
  queries: Query[]
}

interface State {
  subMenuIsOpen: boolean
}

@ErrorHandling
class LayoutCellMenu extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      subMenuIsOpen: false,
    }
  }

  public render() {
    const {queries} = this.props

    return (
      <div className={this.contextMenuClassname}>
        <div className={this.customIndicatorsClassname}>
          {queries && <CustomTimeIndicator queries={queries} />}
        </div>
        {this.renderMenu}
      </div>
    )
  }

  private get renderMenu(): JSX.Element {
    const {isEditable, mode, cell, onDismissEditingAnnotation} = this.props

    if (mode === EDITING && cellSupportsAnnotations(cell.type)) {
      return (
        <div className="dash-graph-context--buttons">
          <div
            className="btn btn-xs btn-success"
            onClick={onDismissEditingAnnotation}
          >
            Done Editing
          </div>
        </div>
      )
    }

    if (isEditable && mode !== EDITING) {
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
    const {queries} = this.props

    if (!queries.length) {
      return
    }

    return (
      <MenuTooltipButton
        icon="pencil"
        menuItems={this.editMenuItems}
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
  private get customIndicatorsClassname(): string {
    const {isEditable} = this.props

    return classnames('dash-graph--custom-indicators', {
      'dash-graph--draggable': isEditable,
    })
  }

  private get editMenuItems(): MenuItem[] {
    const {
      cell,
      dataExists,
      onStartAddingAnnotation,
      onStartEditingAnnotation,
      onCSVDownload,
    } = this.props

    return [
      {
        text: 'Configure',
        action: this.handleEditCell,
        disabled: false,
      },
      {
        text: 'Add Annotation',
        action: onStartAddingAnnotation,
        disabled: !cellSupportsAnnotations(cell.type),
      },
      {
        text: 'Edit Annotations',
        action: onStartEditingAnnotation,
        disabled: !cellSupportsAnnotations(cell.type),
      },
      {
        text: 'Download CSV',
        action: onCSVDownload,
        disabled: !dataExists,
      },
    ]
  }

  private get cloneMenuItems(): MenuItem[] {
    return [{text: 'Clone Cell', action: this.handleCloneCell, disabled: false}]
  }

  private get deleteMenuItems(): MenuItem[] {
    return [{text: 'Confirm', action: this.handleDeleteCell, disabled: false}]
  }

  private handleEditCell = (): void => {
    const {onEdit} = this.props
    onEdit()
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

const mapStateToProps = ({annotations: {mode}}) => ({
  mode,
})

const mapDispatchToProps = dispatch => ({
  onStartAddingAnnotation: bindActionCreators(addingAnnotation, dispatch),
  onStartEditingAnnotation: bindActionCreators(editingAnnotation, dispatch),
  onDismissEditingAnnotation: bindActionCreators(
    dismissEditingAnnotation,
    dispatch
  ),
})

export default connect(mapStateToProps, mapDispatchToProps)(LayoutCellMenu)
