import React, {PropTypes, Component} from 'react'
import ContextMenu from 'src/shared/components/ContextMenu'
import NameableGraphHeader from 'src/shared/components/NameableGraphHeader'

class NameableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isMenuOpen: false,
    }

    this.toggleMenu = ::this.toggleMenu
    this.closeMenu = ::this.closeMenu
  }

  toggleMenu() {
    this.setState({
      isMenuOpen: !this.state.isMenuOpen,
    })
  }

  closeMenu() {
    this.setState({
      isMenuOpen: false,
    })
  }

  render() {
    const {
      cell,
      onEditCell,
      onRenameCell,
      onUpdateCell,
      onDeleteCell,
      onSummonOverlayTechnologies,
      isEditable,
      children,
    } = this.props

    return (
      <div className="dash-graph">
        <NameableGraphHeader
          cell={cell}
          isEditable={isEditable}
          onEditCell={onEditCell}
          onRenameCell={onRenameCell}
          onUpdateCell={onUpdateCell}
        />
        <ContextMenu
          cell={cell}
          onDelete={onDeleteCell}
          onRename={!cell.isEditing && isEditable ? onEditCell : () => {}}
          toggleMenu={this.toggleMenu}
          isOpen={this.state.isMenuOpen}
          isEditable={isEditable}
          handleClickOutside={this.closeMenu}
          onEdit={onSummonOverlayTechnologies}
        />
        <div className="dash-graph--container">
          {children}
        </div>
      </div>
    )
  }
}

const {array, bool, func, node, number, shape, string} = PropTypes

NameableGraph.propTypes = {
  cell: shape({
    name: string.isRequired,
    isEditing: bool,
    x: number.isRequired,
    y: number.isRequired,
    queries: array,
  }).isRequired,
  children: node.isRequired,
  onEditCell: func,
  onRenameCell: func,
  onUpdateCell: func,
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  shouldNotBeEditable: bool,
  isEditable: bool,
}

export default NameableGraph
