// Libraries
import React, {Component, ComponentClass} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import CellHeader from 'src/shared/components/cells/CellHeader'
import CellContext from 'src/shared/components/cells/CellContext'
import ViewComponent from 'src/shared/components/cells/View'
import {ErrorHandling} from 'src/shared/decorators/errors'
import Conditional from 'src/shared/components/Conditional'

// Actions
import {readView} from 'src/dashboards/actions/v2/views'

// Types
import {RemoteDataState, TimeRange} from 'src/types'
import {AppState, ViewType, View, Cell} from 'src/types/v2'

// Styles
import './Cell.scss'

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
  autoRefresh: number
  manualRefresh: number
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onEditCell: () => void
  onZoom: (range: TimeRange) => void
}

type Props = StateProps & DispatchProps & PassedProps

@ErrorHandling
class CellComponent extends Component<Props> {
  public async componentDidMount() {
    const {viewStatus, cell, onReadView} = this.props

    if (viewStatus === RemoteDataState.NotStarted) {
      const dashboardID = cell.dashboardID
      onReadView(dashboardID, cell.id)
    }
  }

  public render() {
    const {onEditCell, onDeleteCell, onCloneCell, cell, view} = this.props

    return (
      <>
        <CellHeader name={this.viewName} note={this.viewNote} />
        {view && (
          <CellContext
            cell={cell}
            view={view}
            onDeleteCell={onDeleteCell}
            onCloneCell={onCloneCell}
            onEditCell={onEditCell}
            onCSVDownload={this.handleCSVDownload}
          />
        )}
        <div className="cell--view" data-testid="cell--view-empty">
          {this.view}
        </div>
      </>
    )
  }

  private get viewName(): string {
    const {view} = this.props

    if (view && view.properties.type !== ViewType.Markdown) {
      return view.name
    }

    return ''
  }

  private get viewNote(): string {
    const {view} = this.props

    if (!view) {
      return ''
    }

    const isMarkdownView = view.properties.type === ViewType.Markdown
    const showNoteWhenEmpty = get(view, 'properties.showNoteWhenEmpty')

    if (isMarkdownView || showNoteWhenEmpty) {
      return ''
    }

    return get(view, 'properties.note', '')
  }

  private get view(): JSX.Element {
    const {
      timeRange,
      autoRefresh,
      manualRefresh,
      onZoom,
      view,
      viewStatus,
      onEditCell,
    } = this.props

    return (
      <Conditional isRendered={viewStatus === RemoteDataState.Done}>
        <ViewComponent
          view={view}
          onZoom={onZoom}
          timeRange={timeRange}
          autoRefresh={autoRefresh}
          manualRefresh={manualRefresh}
          onEditCell={onEditCell}
        />
      </Conditional>
    )
  }

  private handleCSVDownload = (): void => {
    throw new Error('csv download not implemented')
  }
}

const mstp = (state: AppState, ownProps: PassedProps): StateProps => {
  const entry = state.views[ownProps.cell.id]

  if (entry) {
    return {view: entry.view, viewStatus: entry.status}
  }

  return {view: null, viewStatus: RemoteDataState.NotStarted}
}

const mdtp: DispatchProps = {
  onReadView: readView,
}

export default connect(
  mstp,
  mdtp
)(CellComponent) as ComponentClass<PassedProps>
