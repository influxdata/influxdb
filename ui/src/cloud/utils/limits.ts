import {get} from 'lodash'
import {RATE_LIMIT_ERROR_STATUS} from 'src/shared/constants/errors'

export const isLimitError = error => {
  return get(error, 'response.status', '') === RATE_LIMIT_ERROR_STATUS
}

export const extractMessage = error => {
  return get(error, 'response.data.message', '')
}
