import React, {Component, ReactElement} from 'react'
import _ from 'lodash'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import LayoutCellMenu from 'src/shared/components/LayoutCellMenu'
import LayoutCellHeader from 'src/shared/components/LayoutCellHeader'
import {notify} from 'src/shared/actions/notifications'
import {notifyCSVDownloadFailed} from 'src/shared/copy/notifications'
import download from 'src/external/download.js'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {dataToCSV} from 'src/shared/parsing/dataToCSV'
import {timeSeriesToTableGraph} from 'src/utils/timeSeriesTransformers'
import {Cell, CellQuery} from 'src/types/dashboard'
import {TimeSeriesServerResponse} from 'src/types/series'

interface Props {
  cell: Cell
  children: ReactElement<any>
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onSummonOverlayTechnologies: (cell: Cell) => void
  isEditable: boolean
  onCancelEditCell: () => void
  cellData: TimeSeriesServerResponse[]
}

@ErrorHandling
export default class LayoutCell extends Component<Props> {
  public render() {
    const {cell, isEditable, cellData, onDeleteCell, onCloneCell} = this.props

    return (
      <div className="dash-graph">
        <Authorized requiredRole={EDITOR_ROLE}>
          <LayoutCellMenu
            cell={cell}
            queries={this.queries}
            dataExists={!!cellData.length}
            isEditable={isEditable}
            onDelete={onDeleteCell}
            onEdit={this.handleSummonOverlay}
            onClone={onCloneCell}
            onCSVDownload={this.handleCSVDownload}
          />
        </Authorized>
        <LayoutCellHeader cellName={cell.name} isEditable={isEditable} />
        <div className="dash-graph--container">{this.renderGraph}</div>
      </div>
    )
  }

  private get queries(): CellQuery[] {
    const {cell} = this.props
    return _.get(cell, ['queries'], [])
  }

  private get renderGraph(): JSX.Element {
    const {cell, children} = this.props

    if (this.queries.length) {
      const child = React.Children.only(children)
      return React.cloneElement(child, {cellID: cell.id})
    }

    return this.emptyGraph
  }

  private get emptyGraph(): JSX.Element {
    return (
      <div className="graph-empty">
        <Authorized requiredRole={EDITOR_ROLE}>
          <button
            className="no-query--button btn btn-md btn-primary"
            onClick={this.handleSummonOverlay}
          >
            <span className="icon plus" /> Add Data
          </button>
        </Authorized>
      </div>
    )
  }

  private handleSummonOverlay = (): void => {
    const {cell, onSummonOverlayTechnologies} = this.props
    onSummonOverlayTechnologies(cell)
  }

  private handleCSVDownload = (): void => {
    const {cellData, cell} = this.props
    const joinedName = cell.name.split(' ').join('_')
    const {data} = timeSeriesToTableGraph(cellData)

    try {
      download(dataToCSV(data), `${joinedName}.csv`, 'text/plain')
    } catch (error) {
      notify(notifyCSVDownloadFailed())
      console.error(error)
    }
  }
}
