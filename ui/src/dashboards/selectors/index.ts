import {get} from 'lodash'

import {AppState, View} from 'src/types/v2'

export const getView = (state: AppState, id: string): View =>
  get(state, `views.views.${id}.view`)
