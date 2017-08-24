import {NULL} from 'shared/constants/queryFillOptions'

export default function defaultQueryConfig(id) {
  return {
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
    fill: NULL, // handle all fill values as strings, including null
    areTagsAccepted: true,
    rawText: null,
    status: null,
  }
}
