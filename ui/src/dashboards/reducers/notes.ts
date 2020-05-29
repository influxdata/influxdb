import {Action} from 'src/dashboards/actions/notes'
import {NoteEditorMode} from 'src/types/dashboards'

export interface NoteEditorState {
  mode: NoteEditorMode
  note: string
  showNoteWhenEmpty: boolean
  isPreviewing: boolean
  viewID?: string
}

const initialState = (): NoteEditorState => ({
  mode: NoteEditorMode.Adding,
  note: '',
  showNoteWhenEmpty: false,
  isPreviewing: false,
})

const noteEditorReducer = (
  state: NoteEditorState = initialState(),
  action: Action
) => {
  switch (action.type) {
    case 'RESET_NOTE_STATE': {
      return initialState()
    }
    case 'SET_NOTE_STATE': {
      const initialState = action.payload

      return {
        ...state,
        ...initialState,
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
