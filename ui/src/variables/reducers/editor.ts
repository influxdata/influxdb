// Libraries
import {produce} from 'immer'

// Types
import {VariableEditorState} from 'src/types'
import {
  EditorAction,
  CLEAR_VARIABLE_EDITOR,
  CHANGE_VARIABLE_EDITOR_TYPE,
  UPDATE_VARIABLE_EDITOR_NAME,
  UPDATE_VARIABLE_EDITOR_QUERY,
  UPDATE_VARIABLE_EDITOR_MAP,
  UPDATE_VARIABLE_EDITOR_CONSTANT,
} from 'src/variables/actions/creators'

export const initialEditorState = (): VariableEditorState => ({
  name: '',
  selected: 'query',
  argsQuery: null,
  argsMap: null,
  argsConstant: null,
})

export const variableEditorReducer = (
  state: VariableEditorState = initialEditorState(),
  action: EditorAction
): VariableEditorState =>
  produce(state, draftState => {
    switch (action.type) {
      case CLEAR_VARIABLE_EDITOR: {
        return initialEditorState()
      }
      case CHANGE_VARIABLE_EDITOR_TYPE: {
        draftState.selected = action.editorType
        return
      }
      case UPDATE_VARIABLE_EDITOR_NAME: {
        draftState.name = action.name
        return
      }
      case UPDATE_VARIABLE_EDITOR_QUERY: {
        draftState.argsQuery = action.payload
        return
      }
      case UPDATE_VARIABLE_EDITOR_MAP: {
        draftState.argsMap = action.payload
        return
      }
      case UPDATE_VARIABLE_EDITOR_CONSTANT: {
        draftState.argsConstant = action.payload
        return
      }
      default:
        return
    }
  })
