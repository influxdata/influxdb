import {push} from 'react-router-redux'

import {submitNewTask} from 'src/tasks/api/v2'
import {getMe} from 'src/shared/apis/v2/user'
import {getOrganizations} from 'src/shared/apis/v2/organization'
import {notify} from 'src/shared/actions/notifications'
import {taskNotCreated} from 'src/shared/copy/v2/notifications'

export type Action = SetNewScript

export enum ActionTypes {
  SetNewScript = 'SET_NEW_SCRIPT',
}

export interface SetNewScript {
  type: ActionTypes.SetNewScript
  payload: {
    script: string
  }
}

export const setNewScript = (script: string): SetNewScript => ({
  type: ActionTypes.SetNewScript,
  payload: {script},
})

export const saveNewScript = () => async (
  dispatch,
  getState
): Promise<void> => {
  try {
    const {
      links: {tasks: url, me: meUrl, orgs: orgsUrl},
      tasks: {newScript: script},
    } = await getState()

    const user = await getMe(meUrl)
    const orgs = await getOrganizations(orgsUrl)

    await submitNewTask(url, user, orgs[0], script)

    dispatch(setNewScript(''))
    dispatch(push('/tasks'))
  } catch (e) {
    console.error(e)
    dispatch(notify(taskNotCreated()))
  }
}
