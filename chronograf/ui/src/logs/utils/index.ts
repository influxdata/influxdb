import moment from 'moment'

import {DEFAULT_TIME_FORMAT} from 'src/logs/constants'

export const formatTime = (time: number): string => {
  return moment(time).format(DEFAULT_TIME_FORMAT)
}
