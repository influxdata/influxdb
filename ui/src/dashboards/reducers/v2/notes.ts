import {Action} from 'src/dashboards/actions/v2/notes'
import {NoteEditorMode} from 'src/types/v2/dashboards'

export interface NoteEditorState {
  overlayVisible: boolean
  mode: NoteEditorMode
  viewID: string
  toggleVisible: boolean
  note: string
  showNoteWhenEmpty: boolean
  isPreviewing: boolean
}

const initialState = (): NoteEditorState => ({
  overlayVisible: false,
  mode: NoteEditorMode.Adding,
  viewID: null,
  toggleVisible: false,
  note: '',
  showNoteWhenEmpty: false,
  isPreviewing: false,
})

const noteEditorReducer = (
  state: NoteEditorState = initialState(),
  action: Action
) => {
  switch (action.type) {
    case 'OPEN_NOTE_EDITOR': {
      const {initialState} = action.payload

      return {
        ...state,
        ...initialState,
        overlayVisible: true,
        isPreviewing: false,
      }
    }

    case 'CLOSE_NOTE_EDITOR': {
      return {...state, overlayVisible: false}
    }

    case 'SET_IS_PREVIEWING': {
      const {isPreviewing} = action.payload

      return {...state, isPreviewing}
    }

    case 'TOGGLE_SHOW_NOTE_WHEN_EMPTY': {
      const {showNoteWhenEmpty} = state

      return {...state, showNoteWhenEmpty: !showNoteWhenEmpty}
    }

    case 'SET_NOTE': {
      const {note} = action.payload

      return {...state, note}
    }
  }

  return state
}

export default noteEditorReducer
