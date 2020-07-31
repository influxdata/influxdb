import {MeState} from 'src/shared/reducers/me'
import {client} from 'src/utils/api'
import HoneyBadger from 'honeybadger-js'
import {updateReportingContext, gaEvent} from 'src/cloud/utils/reporting'
import {RemoteDataState} from 'src/types'

export const SET_ME = 'SET_ME'

export type Actions = ReturnType<typeof setMe>

export const setMe = (status: RemoteDataState, me?: MeState) =>
  ({
    type: SET_ME,
    payload: {
      status,
      me,
    },
  } as const)

export const getMe = () => async dispatch => {
  try {
    dispatch(setMe(RemoteDataState.Loading))
    const user = await client.users.me()
    updateReportingContext({userID: user.id, userEmail: user.name})

    gaEvent('cloudAppUserDataReady', {
      identity: {
        id: user.id,
        email: user.name,
      },
    })

    HoneyBadger.setContext({
      user_id: user.id,
    })

    dispatch(setMe(RemoteDataState.Done, user as MeState))
  } catch (error) {
    console.error(error)
    dispatch(setMe(RemoteDataState.Error))
  }
}
