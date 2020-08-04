// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import CellHeader from 'src/shared/components/cells/CellHeader'
import CellContext from 'src/shared/components/cells/CellContext'
import ScrollableMarkdown from 'src/shared/components/views/ScrollableMarkdown'
import RefreshingView from 'src/shared/components/RefreshingView'
import {ErrorHandling} from 'src/shared/decorators/errors'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Action
import {setCellMount as setCellMountAction} from 'src/perf/actions'

// Utils
import {getByID} from 'src/resources/selectors'

// Types
import {RemoteDataState, AppState, View, Cell, ResourceType} from 'src/types'

interface StateProps {
  view: View
}

interface OwnProps {
  cell: Cell
  manualRefresh: number
}

interface State {
  inView: boolean
}

interface DispatchProps {
  setCellMount: typeof setCellMountAction
}

type Props = StateProps & OwnProps & DispatchProps

@ErrorHandling
class CellComponent extends Component<Props, State> {
  componentDidMount() {
    const {cell, setCellMount} = this.props

    setCellMount(cell.id, new Date().getTime())
  }

  public render() {
    const {cell, view} = this.props

    return (
      <>
        <CellHeader name={this.viewName} note={this.viewNote}>
          <CellContext
            cell={cell}
            view={view}
            onCSVDownload={this.handleCSVDownload}
          />
        </CellHeader>
        <div
          className="cell--view"
          data-testid={`cell--view-empty ${view?.properties?.type}`}
        >
          {this.view}
        </div>
      </>
    )
  }

  private get viewName(): string {
    const {view} = this.props

    if (view && view.properties && view.properties.type !== 'markdown') {
      return view.name
    }

    return 'Note'
  }

  private get viewNote(): string {
    const {view} = this.props

    if (!view || !view.properties || !view.properties.type) {
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
    const {manualRefresh, view} = this.props

    if (!view || view.status !== RemoteDataState.Done) {
      return <EmptyGraphMessage message="Loading..." />
    }

    if (!view.properties) {
      return null
    }

    if (view.properties.type === 'markdown') {
      return <ScrollableMarkdown text={view.properties.note} />
    }

    return (
      <RefreshingView
        id={view.id}
        properties={view.properties}
        manualRefresh={manualRefresh}
      />
    )
  }

  private handleCSVDownload = (): void => {
    throw new Error('csv download not implemented')
  }
}

const mstp = (state: AppState, ownProps: OwnProps) => {
  const view = getByID<View>(state, ResourceType.Views, ownProps.cell.id)

  return {view}
}

const mdtp = {
  setCellMount: setCellMountAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CellComponent)
