import {reportEvent, toNano} from 'src/cloud/utils/reporting'

interface KeyValue {
  [key: string]: string
}

export const event = (title: string, meta: KeyValue = {}): void => {
  const time = toNano(Date.now())

  reportEvent({
    timestamp: time,
    measurement: title,
    fields: {},
    tags: {...meta},
  })
}
