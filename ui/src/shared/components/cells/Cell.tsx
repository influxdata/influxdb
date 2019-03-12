// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import CellHeader from 'src/shared/components/cells/CellHeader'
import CellContext from 'src/shared/components/cells/CellContext'
import ViewComponent from 'src/shared/components/cells/View'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Utils
import {getView} from 'src/dashboards/selectors'

// Types
import {TimeRange} from 'src/types'
import {AppState, ViewType, View, Cell} from 'src/types/v2'

// Styles
import './Cell.scss'

interface StateProps {
  view: View
}

interface OwnProps {
  cell: Cell
  timeRange: TimeRange
  autoRefresh: number
  manualRefresh: number
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onEditCell: () => void
  onZoom: (range: TimeRange) => void
}

type Props = StateProps & OwnProps

@ErrorHandling
class CellComponent extends Component<Props> {
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
      onEditCell,
    } = this.props

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

const mstp = (state: AppState, ownProps: OwnProps): StateProps => ({
  view: getView(state, ownProps.cell.id),
})

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(CellComponent)
