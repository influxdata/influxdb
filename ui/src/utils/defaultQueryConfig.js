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
    areTagsAccepted: true,
    rawText: null,
    status: null,
  }
}
