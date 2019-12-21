// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import CellHeader from 'src/shared/components/cells/CellHeader'
import CellContext from 'src/shared/components/cells/CellContext'
import ViewComponent from 'src/shared/components/cells/View'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SpinnerContainer} from '@influxdata/clockface'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Utils
import {getView, getViewStatus} from 'src/dashboards/selectors'

// Types
import {AppState, View, Cell, TimeRange, RemoteDataState} from 'src/types'

interface StateProps {
  viewsStatus: RemoteDataState
  view: View
}

interface OwnProps {
  cell: Cell
  timeRange: TimeRange
  manualRefresh: number
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onEditCell: () => void
  onEditNote: (id: string) => void
}

interface State {
  inView: boolean
}

type Props = StateProps & OwnProps

@ErrorHandling
class CellComponent extends Component<Props, State> {
  public render() {
    const {
      onEditCell,
      onEditNote,
      onDeleteCell,
      onCloneCell,
      cell,
      view,
    } = this.props

    return (
      <>
        <CellHeader name={this.viewName} note={this.viewNote}>
          {view && (
            <CellContext
              cell={cell}
              view={view}
              onDeleteCell={onDeleteCell}
              onCloneCell={onCloneCell}
              onEditCell={onEditCell}
              onEditNote={onEditNote}
              onCSVDownload={this.handleCSVDownload}
            />
          )}
        </CellHeader>
        <div className="cell--view" data-testid="cell--view-empty">
          {this.view}
        </div>
      </>
    )
  }

  private get viewName(): string {
    const {view} = this.props

    if (view && view.properties.type !== 'markdown') {
      return view.name
    }

    return 'Note'
  }

  private get viewNote(): string {
    const {view} = this.props

    if (!view) {
      return ''
    }

    const isMarkdownView = view.properties.type === 'markdown'
    const showNoteWhenEmpty = get(view, 'properties.showNoteWhenEmpty')

    if (isMarkdownView || showNoteWhenEmpty) {
      return ''
    }

    return get(view, 'properties.note', '')
  }

  private get view(): JSX.Element {
    const {timeRange, manualRefresh, view, onEditCell, viewsStatus} = this.props

    return (
      <SpinnerContainer
        loading={viewsStatus}
        spinnerComponent={<EmptyGraphMessage message="Loading..." />}
      >
        <ViewComponent
          view={view}
          timeRange={timeRange}
          manualRefresh={manualRefresh}
          onEditCell={onEditCell}
        />
      </SpinnerContainer>
    )
  }

  private handleCSVDownload = (): void => {
    throw new Error('csv download not implemented')
  }
}

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const view = getView(state, ownProps.cell.id)

  const status = getViewStatus(state, ownProps.cell.id)

  return {view, viewsStatus: status}
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(CellComponent)
