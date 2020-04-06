import {getOrg} from 'src/organizations/selectors'
import {store} from 'src/index'
import {CLOUD} from 'src/shared/constants'

export const pageTitleSuffixer = (pageTitles: string[]): string => {
  const state = store.getState()
  const {name} = getOrg(state) || null
  const title = CLOUD ? 'InfluxDB Cloud' : 'InfluxDB'
  const titles = [...pageTitles, name, title]

  return titles.join(' | ')
}
