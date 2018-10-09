// Libraries
import React, {Component, ComponentClass} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import CellMenu from 'src/shared/components/cells/CellMenu'
import CellHeader from 'src/shared/components/cells/CellHeader'
import ViewComponent from 'src/shared/components/cells/View'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {readView} from 'src/dashboards/actions/v2/views'

// Types
import {DashboardQuery, RemoteDataState, Template, TimeRange} from 'src/types'
import {Cell, View, AppState} from 'src/types/v2'

interface StateProps {
  view: View
  viewStatus: RemoteDataState
}

interface DispatchProps {
  onReadView: typeof readView
}

interface PassedProps {
  cell: Cell
  timeRange: TimeRange
  templates: Template[]
  autoRefresh: number
  manualRefresh: number
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onEditCell: () => void
  onZoom: (range: TimeRange) => void
  isEditable: boolean
}

type Props = StateProps & DispatchProps & PassedProps

@ErrorHandling
class CellComponent extends Component<Props> {
  public async componentDidMount() {
    const {viewStatus, cell, onReadView} = this.props

    if (viewStatus === RemoteDataState.NotStarted) {
      onReadView(cell.links.view, cell.viewID)
    }
  }

  public render() {
    const {cell, isEditable, onDeleteCell, onCloneCell, onEditCell} = this.props

    return (
      <div className="dash-graph">
        <CellMenu
          cell={cell}
          dataExists={false}
          queries={this.queries}
          isEditable={isEditable}
          onDelete={onDeleteCell}
          onClone={onCloneCell}
          onEdit={onEditCell}
          onCSVDownload={this.handleCSVDownload}
        />
        <CellHeader cellName={this.viewName} isEditable={isEditable} />
        <div className="dash-graph--container">{this.view}</div>
      </div>
    )
  }

  private get queries(): DashboardQuery[] {
    const {view} = this.props

    return _.get(view, ['properties.queries'], [])
  }

  private get viewName(): string {
    const {view} = this.props
    const viewName = view ? view.name : ''

    return viewName
  }

  private get view(): JSX.Element {
    const {
      templates,
      timeRange,
      autoRefresh,
      manualRefresh,
      onZoom,
      view,
      viewStatus,
      onEditCell,
    } = this.props

    if (viewStatus !== RemoteDataState.Done) {
      return null
    }

    return (
      <ViewComponent
        view={view}
        onZoom={onZoom}
        templates={templates}
        timeRange={timeRange}
        autoRefresh={autoRefresh}
        manualRefresh={manualRefresh}
        onEditCell={onEditCell}
      />
    )
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

const mstp = (state: AppState, ownProps: PassedProps): StateProps => {
  const entry = state.views.views[ownProps.cell.viewID]

  if (entry) {
    return {view: entry.view, viewStatus: entry.status}
  }

  return {view: null, viewStatus: RemoteDataState.NotStarted}
}

const mdtp: DispatchProps = {
  onReadView: readView,
}

export default connect(mstp, mdtp)(CellComponent) as ComponentClass<PassedProps>
