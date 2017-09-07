import {NULL_STRING} from 'shared/constants/queryFillOptions'

const defaultQueryConfig = ({id, isKapacitorRule = false}) => {
  let queryConfig = {
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
  }

  if (!isKapacitorRule) {
    queryConfig = {...queryConfig, fill: NULL_STRING}
  }

  return queryConfig
}

export default defaultQueryConfig
