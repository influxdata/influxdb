// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import NoteEditor from 'src/dashboards/components/NoteEditor'
import {Button, ComponentColor, ComponentStatus} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'

// Actions
import {
  createNoteCell,
  updateViewNote,
  loadNote,
  resetNoteState,
} from 'src/dashboards/actions/notes'
import {notify} from 'src/shared/actions/notifications'

// Utils
import {savingNoteFailed} from 'src/shared/copy/v2/notifications'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {NoteEditorMode} from 'src/types/v2/dashboards'

interface StateProps {
  mode: NoteEditorMode
}

interface DispatchProps {
  onCreateNoteCell: typeof createNoteCell
  onUpdateViewNote: typeof updateViewNote
  resetNote: typeof resetNoteState
  onNotify: typeof notify
  loadNote: typeof loadNote
}

interface RouterProps extends WithRouterProps {
  params: {
    dashboardID: string
    cellID?: string
  }
}

type Props = StateProps & DispatchProps & RouterProps

interface State {
  savingStatus: RemoteDataState
}

class NoteEditorOverlay extends PureComponent<Props, State> {
  public state: State = {savingStatus: RemoteDataState.NotStarted}

  public render() {
    return (
      <div className="note-editor-container">
        <Overlay visible={true}>
          <Overlay.Container maxWidth={900}>
            <Overlay.Heading title={this.overlayTitle} onDismiss={this.close} />
            <Overlay.Body>
              <NoteEditor />
            </Overlay.Body>
            <Overlay.Footer>
              <Button text="Cancel" onClick={this.close} />
              <Button
                text="Save"
                color={ComponentColor.Success}
                status={this.saveButtonStatus}
                onClick={this.handleSave}
              />
            </Overlay.Footer>
          </Overlay.Container>
        </Overlay>
      </div>
    )
  }

  componentDidMount() {
    const {
      params: {cellID},
    } = this.props
    if (cellID) {
      this.props.loadNote(cellID)
    } else {
      this.props.resetNote()
    }
  }

  private get overlayTitle(): string {
    const {mode} = this.props

    let overlayTitle: string

    if (mode === NoteEditorMode.Editing) {
      overlayTitle = 'Edit Note'
    } else {
      overlayTitle = 'Add Note'
    }

    return overlayTitle
  }

  private get saveButtonStatus(): ComponentStatus {
    const {savingStatus} = this.state

    if (savingStatus === RemoteDataState.Loading) {
      return ComponentStatus.Loading
    }

    return ComponentStatus.Default
  }

  private handleSave = async () => {
    const {
      params: {cellID, dashboardID},
      onCreateNoteCell,
      onUpdateViewNote,
      onNotify,
    } = this.props

    this.setState({savingStatus: RemoteDataState.Loading})

    try {
      if (cellID) {
        await onUpdateViewNote(cellID)
      } else {
        await onCreateNoteCell(dashboardID)
      }
      this.close()
    } catch (error) {
      onNotify(savingNoteFailed(error.message))
      console.error(error)
      this.setState({savingStatus: RemoteDataState.Error})
    }
  }

  private close = () => {
    this.props.router.goBack()
  }
}

const mstp = (state: AppState): StateProps => {
  const {mode} = state.noteEditor

  return {mode}
}

const mdtp = {
  onNotify: notify,
  onCreateNoteCell: createNoteCell,
  onUpdateViewNote: updateViewNote,
  resetNote: resetNoteState,
  loadNote,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<StateProps & DispatchProps>(NoteEditorOverlay))
