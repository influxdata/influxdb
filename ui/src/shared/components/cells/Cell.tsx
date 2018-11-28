// Libraries
import React, {Component, ComponentClass} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import CellHeader from 'src/shared/components/cells/CellHeader'
import CellContext from 'src/shared/components/cells/CellContext'
import ViewComponent from 'src/shared/components/cells/View'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {readView} from 'src/dashboards/actions/v2/views'

// Types
import {RemoteDataState, TimeRange} from 'src/types'
import {Cell, View, AppState, ViewType} from 'src/types/v2'

// Styles
import './Cell.scss'

interface StateProps {
  view: View | null
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
      onReadView(cell.links.view, cell.viewID)
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
        <div className="cell--view">{this.view}</div>
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

    if (viewStatus !== RemoteDataState.Done) {
      return null
    }

    return (
      <ViewComponent
        view={view}
        onZoom={onZoom}
        timeRange={timeRange}
        autoRefresh={autoRefresh}
        manualRefresh={manualRefresh}
        onEditCell={onEditCell}
      />
    )
  }

  private handleCSVDownload = (): void => {
    throw new Error('csv download not implemented')
  }
}

const mstp = (state: AppState, ownProps: PassedProps): StateProps => {
  const entry = state.views[ownProps.cell.viewID]

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
