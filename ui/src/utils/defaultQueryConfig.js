import {NULL_STRING} from 'shared/constants/queryFillOptions'

const defaultQueryConfig = ({id, isKapacitorRule = false}) => {
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
    fill: NULL_STRING,
    areTagsAccepted: true,
    rawText: null,
    status: null,
  }

  if (isKapacitorRule) {
    delete queryConfig.fill
  }

  return queryConfig
}

export default defaultQueryConfig
