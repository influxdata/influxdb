import {get} from 'lodash'
import {store} from 'src/index'

export const pageTitleSuffixer = (pageTitles: string[]): string => {
  const state = store.getState()
  const currentOrg = get(state, 'resources.orgs.org.name', null)
  const titles = [...pageTitles, currentOrg, 'InfluxDB 2.0']

  return titles.join(' | ')
}
