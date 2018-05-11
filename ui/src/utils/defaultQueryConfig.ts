import uuid from 'uuid'

import {NULL_STRING} from 'src/shared/constants/queryFillOptions'
import {QueryConfig} from 'src/types'

const defaultQueryConfig = (
  {id, isKapacitorRule = false} = {id: uuid.v4()}
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
  }

  return isKapacitorRule ? queryConfig : {...queryConfig, fill: NULL_STRING}
}

export default defaultQueryConfig
