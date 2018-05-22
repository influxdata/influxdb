import {Action} from 'src/ifql/actions'
import {editor} from 'src/ifql/constants'

const scriptReducer = (
  state: string = editor.DEFAULT_SCRIPT,
  action: Action
): string => {
  switch (action.type) {
    case 'UPDATE_SCRIPT': {
      return action.payload.script
    }
  }

  return state
}

export default scriptReducer
