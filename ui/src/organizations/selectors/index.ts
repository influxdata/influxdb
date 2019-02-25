import {AppState, Organization} from 'src/types/v2'

export const getActiveOrg = (state: AppState): Organization => state.orgs[0]
