import {Action, ActionTypes} from 'src/flux/actions'
import {editor} from 'src/flux/constants'

const scriptReducer = (
  state: string = editor.DEFAULT_SCRIPT,
  action: Action
): string => {
  switch (action.type) {
    case ActionTypes.UpdateScript: {
      return action.payload.script
    }
  }

  return state
}

export default scriptReducer
