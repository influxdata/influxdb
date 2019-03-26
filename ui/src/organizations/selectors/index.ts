import {AppState, Organization} from 'src/types'

export const getActiveOrg = (state: AppState): Organization => state.orgs[0]
