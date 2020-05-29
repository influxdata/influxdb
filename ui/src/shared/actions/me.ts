import {MeState} from 'src/shared/reducers/me'
import {client} from 'src/utils/api'
import {CLOUD} from 'src/shared/constants'
import HoneyBadger from 'honeybadger-js'
import {fireUserDataReady} from 'src/shared/utils/analytics'

export const SET_ME = 'SET_ME'

export type Actions = ReturnType<typeof setMe>

export const setMe = (me: MeState) =>
  ({
    type: SET_ME,
    payload: {
      me,
    },
  } as const)

export const getMe = () => async dispatch => {
  try {
    const user = await client.users.me()

    if (CLOUD) {
      fireUserDataReady(user.id, user.name)
    }

    HoneyBadger.setContext({
      user_id: user.id,
    })

    dispatch(setMe(user as MeState))
  } catch (error) {
    console.error(error)
  }
}
