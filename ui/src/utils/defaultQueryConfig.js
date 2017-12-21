import uuid from 'node-uuid'

import {NULL_STRING} from 'shared/constants/queryFillOptions'

const defaultQueryConfig = (
  {id, isKapacitorRule = false} = {id: uuid.v4()}
) => {
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
  }

  return isKapacitorRule ? queryConfig : {...queryConfig, fill: NULL_STRING}
}

export default defaultQueryConfig
