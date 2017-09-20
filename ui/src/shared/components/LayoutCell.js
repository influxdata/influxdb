import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import LayoutCellMenu from 'shared/components/LayoutCellMenu'
import LayoutCellHeader from 'shared/components/LayoutCellHeader'

class LayoutCell extends Component {
  constructor(props) {
    super(props)
    this.state = {
      cellName: props.cell.name,
      isDeleting: false,
    }
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
    const {cell, children, isEditable} = this.props

    const {cellName, isDeleting} = this.state
    const queries = _.get(cell, ['queries'], [])

    return (
      <div className="dash-graph">
        <LayoutCellMenu
          cell={cell}
          onDeleteClick={this.handleDeleteClick}
          onDelete={this.handleDeleteCell}
          isDeleting={isDeleting}
          isEditable={isEditable}
          handleClickOutside={this.closeMenu}
          onEdit={this.handleSummonOverlay}
        />
        <LayoutCellHeader
          cellName={cellName}
          queries={queries}
          isEditable={isEditable}
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

LayoutCell.propTypes = {
  cell: shape({
    name: string.isRequired,
    isEditing: bool,
    x: number.isRequired,
    y: number.isRequired,
    queries: array,
  }).isRequired,
  children: node.isRequired,
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  isEditable: bool,
  onCancelEditCell: func,
}

export default LayoutCell
