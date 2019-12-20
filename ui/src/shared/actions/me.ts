import {MeState} from 'src/shared/reducers/me'
import {client} from 'src/utils/api'
import HoneyBadger from 'honeybadger-js'

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

    HoneyBadger.setContext({
      user_id: user.id,
    })

    dispatch(setMe(user))
  } catch (error) {
    console.error(error)
  }
}
