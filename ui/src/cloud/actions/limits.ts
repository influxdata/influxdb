// API
import {getReadWriteLimitsAJAX} from 'src/cloud/apis/limits'

// Types
import {AppState} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {readWriteLimitReached} from 'src/shared/copy/notifications'

enum LimitStatus {
  OK = 'ok',
  EXCEEDED = 'exceeded',
}

export const getReadWriteLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
  try {
    const {
      orgs: {org},
    } = getState()

    const limits = await getReadWriteLimitsAJAX(org.id)

    const isReadLimited = limits.read.status === LimitStatus.EXCEEDED
    const isWriteLimited = limits.write.status === LimitStatus.EXCEEDED

    if (isReadLimited || isWriteLimited) {
      dispatch(notify(readWriteLimitReached(isReadLimited, isWriteLimited)))
    }
  } catch (e) {}
}
