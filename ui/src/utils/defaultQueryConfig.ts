import uuid from 'uuid'

import {NULL_STRING} from 'src/shared/constants/queryFillOptions'
import {QueryConfig} from 'src/types'
import {TEMPLATE_RANGE} from 'src/tempVars/constants'

interface DefaultQueryArgs {
  id?: string
  isKapacitorRule?: boolean
}

const defaultQueryConfig = (
  {id, isKapacitorRule = false}: DefaultQueryArgs = {id: uuid.v4()}
): QueryConfig => {
  const queryConfig = {
    id,
    database: null,
    measurement: null,
    retentionPolicy: null,
    fields: [],
    tags: {},
    groupBy: {
      time: null,
      tags: [],
    },
    areTagsAccepted: true,
    rawText: null,
    status: null,
    shifts: [],
    fill: null,
    range: TEMPLATE_RANGE,
    originalQuery: null,
  }

  return isKapacitorRule ? queryConfig : {...queryConfig, fill: NULL_STRING}
}

export default defaultQueryConfig
