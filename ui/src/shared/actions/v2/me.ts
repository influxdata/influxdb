import {MeState} from 'src/shared/reducers/v2/me'
import {getMe as getMeAPI} from 'src/shared/apis/v2/user'

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
    const user = await getMeAPI()

    dispatch(setMe(user))
  } catch (e) {
    console.error(e)
  }
}
