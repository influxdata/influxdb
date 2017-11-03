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
    areTagsAccepted: true,
    rawText: null,
    status: null,
    shift: null,
  }

  return isKapacitorRule ? queryConfig : {...queryConfig, fill: NULL_STRING}
}

export default defaultQueryConfig
