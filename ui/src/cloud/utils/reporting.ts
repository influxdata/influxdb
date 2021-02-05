import {useState, useEffect} from 'react'
import {isEmpty} from 'lodash'

import {
  reportPoints as reportPointsAPI,
  Point,
  PointTags,
  PointFields,
} from 'src/cloud/apis/reporting'

import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {GIT_SHA} from 'src/shared/constants'
export {Point, PointTags, PointFields} from 'src/cloud/apis/reporting'

let reportingTags = {}
let reportingPoints = []
let reportDecayTimeout = null
let reportMaxTimeout = null

const REPORT_DECAY = 500 // number of miliseconds to wait after last event before sending
const REPORT_MAX_WAIT = 5000 // max number of miliseconds to wait between sends
const REPORT_MAX_LENGTH = 300 // max number of events to queue before sending

interface KeyValue {
  [key: string]: string
}

export const updateReportingContext = (properties: KeyValue) => {
  reportingTags = {...reportingTags, ...properties}
}

export const toNano = (ms: number) => Math.round(ms * 1000000)

// NOTE: typescript can't follow the API results for flags,
// so we need to convert them to strings here
const cleanTags = (data: Point): Point => {
  return {
    ...data,
    tags: Object.entries(data.tags).reduce((acc, [key, val]) => {
      if (typeof val === 'boolean') {
        acc[key] = val ? 'true' : 'false'
        return acc
      }

      if (!isNaN(parseFloat(val as any))) {
        acc[key] = '' + val
        return acc
      }

      acc[key] = val
      return acc
    }, {}),
  }
}

const pooledEvent = ({timestamp, measurement, fields, tags}: Point) => {
  if (isEmpty(fields)) {
    fields = {source: 'ui'}
  }

  reportingPoints.push(
    cleanTags({
      measurement,
      tags: {...reportingTags, ...tags, version: GIT_SHA},
      fields,
      timestamp,
    })
  )

  if (!!reportDecayTimeout) {
    clearTimeout(reportDecayTimeout)
    reportDecayTimeout = null
  }

  if (reportingPoints.length >= REPORT_MAX_LENGTH) {
    if (!!reportMaxTimeout) {
      clearTimeout(reportMaxTimeout)
      reportMaxTimeout = null
    }

    reportPointsAPI({
      points: reportingPoints.slice(),
    })

    reportingPoints = []

    return
  }

  if (!reportMaxTimeout) {
    reportMaxTimeout = setTimeout(() => {
      reportMaxTimeout = null

      // points already cleared
      if (!reportingPoints.length) {
        return
      }

      clearTimeout(reportDecayTimeout)
      reportDecayTimeout = null

      reportPointsAPI({
        points: reportingPoints.slice(),
      })

      reportingPoints = []
    }, REPORT_MAX_WAIT)
  }

  reportDecayTimeout = setTimeout(() => {
    reportPointsAPI({
      points: reportingPoints.slice(),
    })

    reportingPoints = []
  }, REPORT_DECAY)
}

export const gaEvent = (event: string, payload: object = {}) => {
  window.dataLayer = window.dataLayer || []
  window.dataLayer.push({
    event,
    ...payload,
  })
}

export const event = (
  measurement: string,
  meta: PointTags = {},
  values: PointFields = {}
): void => {
  let time = meta.time ? new Date(meta.time).valueOf() : Date.now()

  if (isNaN(time)) {
    time = Date.now()
  }

  delete meta.time

  if (isFlagEnabled('streamEvents')) {
    /* eslint-disable no-console */
    console.log(`Event:  [ ${measurement} ]`)
    if (Object.keys(meta).length) {
      console.log('tags')
      console.log(
        Object.entries(meta)
          .map(([k, v]) => `        ${k}: ${v}`)
          .join('\n')
      )
      console.log('fields')
      console.log(
        Object.entries(values)
          .map(([k, v]) => `        ${k}: ${v}`)
          .join('\n')
      )
    }
    /* eslint-enable no-console */
  }

  gaEvent(measurement, {...values, ...meta})

  pooledEvent({
    timestamp: toNano(time),
    measurement,
    fields: {
      source: 'ui',
      ...values,
    },
    tags: {...meta},
  })
}

export const useLoadTimeReporting = (measurement: string) => {
  const [loadStartTime] = useState(Date.now())
  useEffect(() => {
    event(measurement, {
      time: loadStartTime,
    })
  }, [measurement, loadStartTime])
}
