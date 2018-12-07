import {authorization} from './mockData'

// Types
import {Authorization} from 'src/api'

export const getAuthorizations = async (): Promise<Authorization[]> => {
  return Promise.resolve([authorization, {...authorization, id: '1'}])
}
