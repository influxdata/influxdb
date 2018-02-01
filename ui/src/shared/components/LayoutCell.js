import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import LayoutCellMenu from 'shared/components/LayoutCellMenu'
import LayoutCellHeader from 'shared/components/LayoutCellHeader'
import {errorThrown} from 'shared/actions/errors'
import {dashboardtoCSV} from 'shared/parsing/resultsToCSV'
import download from 'src/external/download.js'

class LayoutCell extends Component {
  handleDeleteCell = cell => () => {
    this.props.onDeleteCell(cell)
  }

  handleSummonOverlay = cell => () => {
    this.props.onSummonOverlayTechnologies(cell)
  }

  handleCSVDownload = cell => () => {
    const joinedName = cell.name.split(' ').join('_')
    const {celldata} = this.props
    try {
      download(dashboardtoCSV(celldata), `${joinedName}.csv`, 'text/plain')
    } catch (error) {
      errorThrown(error, 'Unable to download .csv file')
      console.error(error)
    }
  }

  render() {
    const {cell, children, isEditable, celldata} = this.props

    const queries = _.get(cell, ['queries'], [])

    return (
      <div className="dash-graph">
        <Authorized requiredRole={EDITOR_ROLE}>
          <LayoutCellMenu
            cell={cell}
            queries={queries}
            dataExists={!!celldata.length}
            isEditable={isEditable}
            onDelete={this.handleDeleteCell}
            onEdit={this.handleSummonOverlay}
            onCSVDownload={this.handleCSVDownload}
          />
        </Authorized>
        <LayoutCellHeader cellName={cell.name} isEditable={isEditable} />
        <div className="dash-graph--container">
          {queries.length
            ? children
            : <div className="graph-empty">
                <Authorized requiredRole={EDITOR_ROLE}>
                  <button
                    className="no-query--button btn btn-md btn-primary"
                    onClick={this.handleSummonOverlay(cell)}
                  >
                    <span className="icon plus" /> Add Graph
                  </button>
                </Authorized>
              </div>}
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, node, number, shape, string} = PropTypes

LayoutCell.propTypes = {
  cell: shape({
    name: string.isRequired,
    isEditing: bool,
    x: number.isRequired,
    y: number.isRequired,
    queries: arrayOf(shape()),
  }).isRequired,
  children: node.isRequired,
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  isEditable: bool,
  onCancelEditCell: func,
  celldata: arrayOf(shape()),
}

export default LayoutCell
