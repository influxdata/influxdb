// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import CellMenu from 'src/shared/components/cells/CellMenu'
import CellHeader from 'src/shared/components/cells/CellHeader'
import ViewComponent from 'src/shared/components/cells/View'

// APIs
import {getView} from 'src/dashboards/apis/v2/view'

// Types
import {CellQuery, RemoteDataState, Template, TimeRange} from 'src/types'
import {Cell, View, ViewShape} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  cell: Cell
  timeRange: TimeRange
  templates: Template[]
  autoRefresh: number
  manualRefresh: number
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onZoom: (range: TimeRange) => void
  isEditable: boolean
}

interface State {
  view: View
  loading: RemoteDataState
}

@ErrorHandling
export default class CellComponent extends Component<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      view: null,
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    const {cell} = this.props
    const view = await getView(cell.links.view)
    this.setState({view, loading: RemoteDataState.Done})
  }

  public render() {
    const {cell, isEditable, onDeleteCell, onCloneCell} = this.props

    return (
      <div className="dash-graph">
        <CellMenu
          cell={cell}
          dataExists={false}
          queries={this.queries}
          isEditable={isEditable}
          onDelete={onDeleteCell}
          onClone={onCloneCell}
          onEdit={this.handleSummonOverlay}
          onCSVDownload={this.handleCSVDownload}
        />
        <CellHeader cellName="" isEditable={isEditable} />
        <div className="dash-graph--container">{this.view}</div>
      </div>
    )
  }

  private get queries(): CellQuery[] {
    const {view} = this.state
    return _.get(view, ['properties.queries'], [])
  }

  private get view(): JSX.Element {
    const {
      templates,
      timeRange,
      autoRefresh,
      manualRefresh,
      onZoom,
    } = this.props
    const {view, loading} = this.state

    if (loading !== RemoteDataState.Done) {
      return null
    }

    if (view.properties.shape === ViewShape.Empty) {
      return this.emptyGraph
    }

    return (
      <ViewComponent
        view={view}
        onZoom={onZoom}
        templates={templates}
        timeRange={timeRange}
        autoRefresh={autoRefresh}
        manualRefresh={manualRefresh}
      />
    )
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
    // TODO: add back in once CEO is refactored
  }

  private handleCSVDownload = (): void => {
    // TODO: get data from link
    // const {cellData, cell} = this.props
    // const joinedName = cell.name.split(' ').join('_')
    // const {data} = timeSeriesToTableGraph(cellData)
    // try {
    //   download(dataToCSV(data), `${joinedName}.csv`, 'text/plain')
    // } catch (error) {
    //   notify(csvDownloadFailed())
    //   console.error(error)
    // }
  }
}
