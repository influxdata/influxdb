// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import NoteEditor from 'src/dashboards/components/NoteEditor'
import {
  OverlayBody,
  OverlayHeading,
  OverlayTechnology,
  OverlayContainer,
  Button,
  ComponentColor,
  ComponentStatus,
} from 'src/clockface'

// Actions
import {
  closeNoteEditor,
  createNoteCell,
  updateViewNote,
} from 'src/dashboards/actions/v2/notes'
import {notify} from 'src/shared/actions/notifications'

// Utils
import {savingNoteFailed} from 'src/shared/copy/v2/notifications'

// Styles
import 'src/dashboards/components/NoteEditorContainer.scss'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {NoteEditorMode} from 'src/types/v2/dashboards'

interface StateProps {
  mode: NoteEditorMode
  overlayVisible: boolean
  viewID: string
}

interface DispatchProps {
  onHide: typeof closeNoteEditor
  onCreateNoteCell: (dashboardID: string) => Promise<void>
  onUpdateViewNote: () => Promise<void>
  onNotify: typeof notify
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps & WithRouterProps

interface State {
  savingStatus: RemoteDataState
}

class NoteEditorContainer extends PureComponent<Props, State> {
  public state: State = {savingStatus: RemoteDataState.NotStarted}

  public render() {
    const {onHide, overlayVisible} = this.props

    return (
      <div className="note-editor-container">
        <OverlayTechnology visible={overlayVisible}>
          <OverlayContainer>
            <OverlayHeading title={this.overlayTitle}>
              <div className="create-source-overlay--heading-buttons">
                <Button text="Cancel" onClick={onHide} />
                <Button
                  text="Save"
                  color={ComponentColor.Success}
                  status={this.saveButtonStatus}
                  onClick={this.handleSave}
                />
              </div>
            </OverlayHeading>
            <OverlayBody>
              <NoteEditor />
            </OverlayBody>
          </OverlayContainer>
        </OverlayTechnology>
      </div>
    )
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
      viewID,
      onCreateNoteCell,
      onUpdateViewNote,
      onHide,
      onNotify,
    } = this.props

    const dashboardID = this.props.params.dashboardID

    this.setState({savingStatus: RemoteDataState.Loading})

    try {
      if (viewID) {
        await onUpdateViewNote()
      } else {
        await onCreateNoteCell(dashboardID)
      }

      this.setState({savingStatus: RemoteDataState.NotStarted}, onHide)
    } catch (error) {
      onNotify(savingNoteFailed(error.message))
      console.error(error)
      this.setState({savingStatus: RemoteDataState.Error})
    }
  }
}

const mstp = (state: AppState) => {
  const {mode, overlayVisible, viewID} = state.noteEditor

  return {mode, overlayVisible, viewID}
}

const mdtp = {
  onHide: closeNoteEditor,
  onNotify: notify,
  onCreateNoteCell: createNoteCell as any,
  onUpdateViewNote: updateViewNote as any,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<StateProps & DispatchProps & OwnProps>(NoteEditorContainer))
