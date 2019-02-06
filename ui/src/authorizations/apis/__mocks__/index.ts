import {authorization} from './data'

// Types
import {Authorization} from '@influxdata/influx'

export const getAuthorizations = async (): Promise<Authorization[]> => {
  return Promise.resolve([authorization, {...authorization, id: '1'}])
}
