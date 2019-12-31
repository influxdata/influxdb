// Types
import {AppState} from 'src/types'

export const getAll = <R>({resources}: AppState, resource): R => {
  const {allIDs} = resources[resource]
  const {byID} = resources[resource]
  return allIDs.map(id => byID[id])
}
