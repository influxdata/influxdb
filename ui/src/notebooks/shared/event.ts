import {reportEvent, toNano} from 'src/cloud/utils/reporting'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

interface KeyValue {
  [key: string]: string
}

export const event = (title: string, meta: KeyValue = {}): void => {
  const time = toNano(Date.now())

  if (isFlagEnabled('streamEvents')) {
    /* eslint-disable no-console */
    console.log(`Event:  [ ${title} ]`)
    if (Object.keys(meta).length) {
      console.log(
        Object.entries(meta)
          .map(([k, v]) => `        ${k}: ${v}`)
          .join('\n')
      )
    }
    /* eslint-enable no-console */
  }

  reportEvent({
    timestamp: time,
    measurement: title,
    fields: {},
    tags: {...meta},
  })
}
