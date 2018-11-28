// Libraries
import {get, isUndefined} from 'lodash'

// Actions
import {createCellWithView} from 'src/dashboards/actions/v2'
import {updateView} from 'src/dashboards/actions/v2/views'

// Utils
import {createView} from 'src/shared/utils/view'

// Types
import {GetState} from 'src/types/v2'
import {NoteEditorMode, MarkdownView, ViewType} from 'src/types/v2/dashboards'
import {NoteEditorState} from 'src/dashboards/reducers/v2/notes'

export type Action =
  | OpenNoteEditorAction
  | CloseNoteEditorAction
  | SetIsPreviewingAction
  | ToggleShowNoteWhenEmptyAction
  | SetNoteAction

interface OpenNoteEditorAction {
  type: 'OPEN_NOTE_EDITOR'
  payload: {initialState: Partial<NoteEditorState>}
}

export const openNoteEditor = (
  initialState: Partial<NoteEditorState>
): OpenNoteEditorAction => ({
  type: 'OPEN_NOTE_EDITOR',
  payload: {initialState},
})

export const addNote = (): OpenNoteEditorAction => ({
  type: 'OPEN_NOTE_EDITOR',
  payload: {
    initialState: {
      mode: NoteEditorMode.Adding,
      viewID: null,
      toggleVisible: false,
      note: '',
    },
  },
})

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

export const createNoteCell = (dashboardID: string) => async (
  dispatch,
  getState: GetState
) => {
  const dashboard = getState().dashboards.find(d => d.id === dashboardID)

  if (!dashboard) {
    throw new Error(`could not find dashboard with id "${dashboardID}"`)
  }

  const {note} = getState().noteEditor
  const view = createView<MarkdownView>(ViewType.Markdown)

  view.properties.note = note

  return dispatch(createCellWithView(dashboard, view))
}

export const updateViewNote = () => async (dispatch, getState: GetState) => {
  const state = getState()
  const {note, showNoteWhenEmpty, viewID} = state.noteEditor
  const view: any = get(state, `views.${viewID}.view`)

  if (!view) {
    throw new Error(`could not find view with id "${viewID}"`)
  }

  if (isUndefined(view.properties.note)) {
    throw new Error(
      `view type "${view.properties.type}" does not support notes`
    )
  }

  const updatedView = {
    ...view,
    properties: {...view.properties, note, showNoteWhenEmpty},
  }

  return dispatch(updateView(view.links.self, updatedView))
}
