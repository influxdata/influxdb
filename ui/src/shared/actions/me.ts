import {MeState} from 'src/shared/reducers/me'
import {client} from 'src/utils/api'
import HoneyBadger from 'honeybadger-js'
import {updateReportingContext, gaEvent} from 'src/cloud/utils/reporting'

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
    updateReportingContext({userID: user.id, userEmail: user.name})

    gaEvent('cloudAppUserDataReady', {
      identity: {
        id: user.id,
        email: user.name,
      },
    })

    updateReportingContext({
      userID: user.id,
    })
    HoneyBadger.setContext({
      user_id: user.id,
    })

    dispatch(setMe(user as MeState))
  } catch (error) {
    console.error(error)
  }
}
