import React, {Component, ReactElement} from 'react'
import _ from 'lodash'

import LayoutCellMenu from 'src/shared/components/LayoutCellMenu'
import LayoutCellHeader from 'src/shared/components/LayoutCellHeader'
import {notify} from 'src/shared/actions/notifications'
import {notifyCSVDownloadFailed} from 'src/shared/copy/notifications'
import download from 'src/external/download.js'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {dataToCSV} from 'src/shared/parsing/dataToCSV'
import {timeSeriesToTableGraph} from 'src/utils/timeSeriesTransformers'
import {PREDEFINED_TEMP_VARS} from 'src/shared/constants'

import {Cell, CellQuery, Template} from 'src/types/'
import {TimeSeriesServerResponse} from 'src/types/series'

interface Props {
  cell: Cell
  children: ReactElement<any>
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onSummonOverlayTechnologies: (cell: Cell) => void
  isEditable: boolean
  cellData: TimeSeriesServerResponse[]
  templates: Template[]
}

@ErrorHandling
export default class LayoutCell extends Component<Props> {
  public render() {
    const {cell, isEditable, cellData, onDeleteCell, onCloneCell} = this.props

    return (
      <div className="dash-graph">
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
        <LayoutCellHeader cellName={this.cellName} isEditable={isEditable} />
        <div className="dash-graph--container">{this.renderGraph}</div>
      </div>
    )
  }

  private get cellName(): string {
    const {
      cell: {name},
    } = this.props
    return this.replaceTemplateVariables(name)
  }

  private get userDefinedTemplateVariables(): Template[] {
    const {templates} = this.props
    return templates.filter(temp => {
      const isPredefinedTempVar: boolean = !!PREDEFINED_TEMP_VARS.find(
        t => t === temp.tempVar
      )
      return !isPredefinedTempVar
    })
  }

  private replaceTemplateVariables = (str: string): string => {
    const isTemplated: boolean = _.get(str.match(/:/g), 'length', 0) >= 2 // tempVars are wrapped in :

    if (isTemplated) {
      const renderedString = _.reduce<Template, string>(
        this.userDefinedTemplateVariables,
        (acc, template) => {
          const {tempVar} = template
          const templateValue = template.values.find(v => v.localSelected)
          const value = _.get(templateValue, 'value', tempVar)
          const regex = new RegExp(tempVar, 'g')
          return acc.replace(regex, value)
        },
        str
      )

      return renderedString
    }

    return str
  }

  private get queries(): CellQuery[] {
    const {cell} = this.props
    return _.get(cell, ['queries'], [])
  }

  private get renderGraph(): JSX.Element {
    const {cell, children} = this.props

    if (this.queries.length) {
      const child = React.Children.only(children)
      return React.cloneElement(child, {cellID: cell.i})
    }

    return this.emptyGraph
  }

  private get emptyGraph(): JSX.Element {
    return (
      <div className="graph-empty">
        <button
          className="no-query--button btn btn-md btn-primary"
          onClick={this.handleSummonOverlay}
        >
          <span className="icon plus" /> Add Data
        </button>
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
