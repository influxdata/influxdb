// Libraries
import {get} from 'lodash'

// Actions
import {createCellWithView} from 'src/cells/actions/thunks'
import {updateView} from 'src/views/actions/thunks'

// Utils
import {createView} from 'src/views/helpers'
import {getByID} from 'src/resources/selectors'

// Types
import {
  GetState,
  MarkdownViewProperties,
  NoteEditorMode,
  ResourceType,
  Dashboard,
  View,
} from 'src/types'
import {NoteEditorState} from 'src/dashboards/reducers/notes'
import {Dispatch} from 'react'

export type Action =
  | CloseNoteEditorAction
  | SetIsPreviewingAction
  | ToggleShowNoteWhenEmptyAction
  | SetNoteAction
  | SetNoteStateAction
  | ResetNoteStateAction

interface CloseNoteEditorAction {
  type: 'CLOSE_NOTE_EDITOR'
}

export const closeNoteEditor = (): CloseNoteEditorAction => ({
  type: 'CLOSE_NOTE_EDITOR',
})

interface SetIsPreviewingAction {
  type: 'SET_IS_PREVIEWING'
  payload: {isPreviewing: boolean}
}

export const setIsPreviewing = (
  isPreviewing: boolean
): SetIsPreviewingAction => ({
  type: 'SET_IS_PREVIEWING',
  payload: {isPreviewing},
})

interface ToggleShowNoteWhenEmptyAction {
  type: 'TOGGLE_SHOW_NOTE_WHEN_EMPTY'
}

export const toggleShowNoteWhenEmpty = (): ToggleShowNoteWhenEmptyAction => ({
  type: 'TOGGLE_SHOW_NOTE_WHEN_EMPTY',
})

interface SetNoteAction {
  type: 'SET_NOTE'
  payload: {note: string}
}

export const setNote = (note: string): SetNoteAction => ({
  type: 'SET_NOTE',
  payload: {note},
})

export const createNoteCell = (dashboardID: string) => (
  dispatch: Dispatch<Action | ReturnType<typeof createCellWithView>>,
  getState: GetState
) => {
  const dashboard = getByID<Dashboard>(
    getState(),
    ResourceType.Dashboards,
    dashboardID
  )

  if (!dashboard) {
    throw new Error(`could not find dashboard with id "${dashboardID}"`)
  }

  const {note} = getState().noteEditor
  const view = createView<MarkdownViewProperties>('markdown')

  view.properties.note = note

  return dispatch(createCellWithView(dashboard.id, view))
}

export interface ResetNoteStateAction {
  type: 'RESET_NOTE_STATE'
}

export const resetNoteState = (): ResetNoteStateAction => ({
  type: 'RESET_NOTE_STATE',
})

export interface SetNoteStateAction {
  type: 'SET_NOTE_STATE'
  payload: Partial<NoteEditorState>
}

export const setNoteState = (
  noteState: Partial<NoteEditorState>
): SetNoteStateAction => ({
  type: 'SET_NOTE_STATE',
  payload: noteState,
})

export const loadNote = (id: string) => (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const state = getState()
  const currentViewState = getByID<View>(state, ResourceType.Views, id)

  if (!currentViewState) {
    return
  }

  const view = currentViewState

  const note: string = get(view, 'properties.note', '')
  const showNoteWhenEmpty: boolean = get(
    view,
    'properties.showNoteWhenEmpty',
    false
  )

  const initialState = {
    viewID: view.id,
    note,
    showNoteWhenEmpty,
    mode: NoteEditorMode.Editing,
  }

  dispatch(setNoteState(initialState))
}

export const updateViewNote = (id: string) => (
  dispatch: Dispatch<Action | ReturnType<typeof updateView>>,
  getState: GetState
) => {
  const state = getState()
  const {note, showNoteWhenEmpty} = state.noteEditor
  const view = getByID<View>(state, ResourceType.Views, id)

  if (view.properties.type === 'check') {
    throw new Error(
      `view type "${view.properties.type}" does not support notes`
    )
  }

  const updatedView = {
    ...view,
    properties: {...view.properties, note, showNoteWhenEmpty},
  }

  return dispatch(updateView(view.dashboardID, updatedView))
}
