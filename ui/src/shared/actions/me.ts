import HoneyBadger from 'honeybadger-js'
import {MeState} from 'src/shared/reducers/me'
import {CLOUD} from 'src/shared/constants'
import {client} from 'src/utils/api'

export enum ActionTypes {
  SetMe = 'SET_ME',
}

export interface SetMe {
  type: ActionTypes.SetMe
  payload: {
    me: MeState
  }
}

export type Actions = SetMe

export const setMe = me => ({
  type: ActionTypes.SetMe,
  payload: {
    me,
  },
})

export const getMe = () => async dispatch => {
  try {
    const user = await client.users.me()

    if (CLOUD) {
      window.context.identity.userID = user.id
      window.context.identity.username = user.name
    }

    HoneyBadger.setContext({
      user_id: user.id,
    })

    dispatch(setMe(user))
  } catch (error) {
    console.error(error)
  }
}
