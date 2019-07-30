// Types
import {NotificationRule} from 'src/types'

interface OverlayState {
  schedule: 'cron' | 'every'
}

export type State = NotificationRule & OverlayState
export type Actions =
  | {type: 'UPDATE_RULE'; rule: NotificationRule}
  | {type: 'SET_ACTIVE_SCHEDULE'; schedule: 'cron' | 'every'}

export const reducer = (state: State, action: Actions) => {
  switch (action.type) {
    case 'UPDATE_RULE':
      const {rule} = action
      return {...state, ...rule}
    case 'SET_ACTIVE_SCHEDULE':
      const {schedule} = action
      return {...state, schedule}

    default:
      throw new Error('unhandled reducer action in <NewRuleOverlay/>')
  }
}
