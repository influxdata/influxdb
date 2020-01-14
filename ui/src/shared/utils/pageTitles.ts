import {getOrg} from 'src/organizations/selectors'
import {store} from 'src/index'

export const pageTitleSuffixer = (pageTitles: string[]): string => {
  const state = store.getState()
  const {name} = getOrg(state) || null
  const titles = [...pageTitles, name, 'InfluxDB 2.0']

  return titles.join(' | ')
}
