// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import WidgetCell from 'src/shared/components/WidgetCell'
import LayoutCell from 'src/shared/components/LayoutCell'
import RefreshingGraph from 'src/shared/components/RefreshingGraph'

// Utils
import {buildQueriesForLayouts} from 'src/utils/buildQueriesForLayouts'

// Constants
import {IS_STATIC_LEGEND} from 'src/shared/constants'

// Types
import {TimeRange, Cell, Template, Source} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  cell: Cell
  timeRange: TimeRange
  templates: Template[]
  source: Source
  sources: Source[]
  host: string
  autoRefresh: number
  isEditable: boolean
  manualRefresh: number
  onZoom: () => void
  onDeleteCell: () => void
  onCloneCell: () => void
  onSummonOverlayTechnologies: () => void
}

@ErrorHandling
class Layout extends Component<Props> {
  public state = {
    cellData: [],
  }

  public render() {
    const {
      cell,
      host,
      source,
      sources,
      onZoom,
      timeRange,
      autoRefresh,
      manualRefresh,
      templates,
      isEditable,
      onCloneCell,
      onDeleteCell,
      onSummonOverlayTechnologies,
    } = this.props
    const {cellData} = this.state

    return (
      <LayoutCell
        cell={cell}
        cellData={cellData}
        templates={templates}
        isEditable={isEditable}
        onCloneCell={onCloneCell}
        onDeleteCell={onDeleteCell}
        onSummonOverlayTechnologies={onSummonOverlayTechnologies}
      >
        {cell.isWidget ? (
          <WidgetCell cell={cell} timeRange={timeRange} source={source} />
        ) : (
          <RefreshingGraph
            onZoom={onZoom}
            axes={cell.axes}
            type={cell.type}
            inView={cell.inView}
            colors={cell.colors}
            tableOptions={cell.tableOptions}
            fieldOptions={cell.fieldOptions}
            decimalPlaces={cell.decimalPlaces}
            timeRange={timeRange}
            templates={templates}
            autoRefresh={autoRefresh}
            manualRefresh={manualRefresh}
            staticLegend={IS_STATIC_LEGEND(cell.legend)}
            grabDataForDownload={this.grabDataForDownload}
            queries={buildQueriesForLayouts(cell, timeRange, host)}
            source={this.getSource(cell, source, sources, source)}
          />
        )}
      </LayoutCell>
    )
  }

  private grabDataForDownload = cellData => {
    this.setState({cellData})
  }

  private getSource = (cell, source, sources, defaultSource): Source => {
    const s = _.get(cell, ['queries', '0', 'source'], null)

    if (!s) {
      return source
    }

    return sources.find(src => src.links.self === s) || defaultSource
  }
}

export default Layout
