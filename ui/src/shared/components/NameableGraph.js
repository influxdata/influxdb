import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import NameableGraphHeader from 'shared/components/NameableGraphHeader'
import ContextMenu from 'shared/components/ContextMenu'

class NameableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isMenuOpen: false,
      cellName: props.cell.name,
      isDeleting: false,
    }
  }

  toggleMenu = () => {
    this.setState({
      isMenuOpen: !this.state.isMenuOpen,
    })
  }

  handleRenameCell = e => {
    const cellName = e.target.value
    this.setState({cellName})
  }

  handleCancelEdit = cellID => {
    const {cell, onCancelEditCell} = this.props
    this.setState({cellName: cell.name})
    onCancelEditCell(cellID)
  }

  closeMenu = () => {
    this.setState({
      isMenuOpen: false,
      isDeleting: false,
    })
  }

  handleDeleteClick = () => {
    this.setState({isDeleting: true})
  }

  handleDeleteCell = cell => () => {
    this.props.onDeleteCell(cell)
  }

  handleSummonOverlay = cell => () => {
    this.props.onSummonOverlayTechnologies(cell)
  }

  render() {
    const {cell, children, isEditable, onEditCell, onUpdateCell} = this.props

    const {cellName, isMenuOpen, isDeleting} = this.state
    const queries = _.get(cell, ['queries'], [])

    return (
      <div className="dash-graph">
        <NameableGraphHeader
          cell={cell}
          cellName={cellName}
          isEditable={isEditable}
          onUpdateCell={onUpdateCell}
          onRenameCell={this.handleRenameCell}
          onCancelEditCell={this.handleCancelEdit}
        />
        <ContextMenu
          cell={cell}
          onDeleteClick={this.handleDeleteClick}
          onDelete={this.handleDeleteCell}
          onRename={!cell.isEditing && isEditable ? onEditCell : () => {}}
          toggleMenu={this.toggleMenu}
          isDeleting={isDeleting}
          isOpen={isMenuOpen}
          isEditable={isEditable}
          handleClickOutside={this.closeMenu}
          onEdit={this.handleSummonOverlay}
        />
        <div className="dash-graph--container">
          {queries.length
            ? children
            : <div className="graph-empty">
                <button
                  className="no-query--button btn btn-md btn-primary"
                  onClick={this.handleSummonOverlay(cell)}
                >
                  Add Graph
                </button>
              </div>}
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
  isEditable: bool,
  onCancelEditCell: func,
}

export default NameableGraph
