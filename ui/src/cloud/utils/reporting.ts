import {useState, useEffect} from 'react'
import {isEmpty} from 'lodash'

import {
  reportPoints as reportPointsAPI,
  Point,
  PointTags,
  PointFields,
} from 'src/cloud/apis/reporting'

import {fireEvent} from 'src/shared/utils/analytics'

export {Point, PointTags, PointFields} from 'src/cloud/apis/reporting'

let reportingTags = {}
let reportingPoints = []
let reportDecayTimeout = null
let reportMaxTimeout = null

const REPORT_DECAY = 500 // number of miliseconds to wait after last event before sending
const REPORT_MAX_WAIT = 5000 // max number of miliseconds to wait between sends
const REPORT_MAX_LENGTH = 300 // max number of events to queue before sending

export const toNano = (ms: number) => Math.round(ms * 1000000)

interface KeyValue {
  [key: string]: string
}

export const updateReportingContext = (properties: KeyValue) => {
  reportingTags = {...reportingTags, ...properties}
}

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

      if (!isNaN(parseFloat(val))) {
        acc[key] = '' + val
        return acc
      }

      acc[key] = val
      return acc
    }, {}),
  }
}

export const reportEvent = ({
  timestamp = toNano(Date.now()),
  measurement,
  fields,
  tags,
}: Point) => {
  if (isEmpty(fields)) {
    fields = {source: 'ui'}
  }

  fireEvent(measurement, {...reportingTags, ...tags})

  reportingPoints.push(
    cleanTags({
      measurement,
      tags: {...reportingTags, ...tags},
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

export const reportQueryPerformanceEvent = ({
  timestamp,
  fields,
  tags,
}: {
  timestamp: number
  fields: PointFields
  tags: PointTags
}) => {
  reportEvent({timestamp, measurement: 'UIQueryPerformance', fields, tags})
}

export const reportSimpleQueryPerformanceEvent = (
  event: string,
  additionalTags: object = {}
) => {
  reportQueryPerformanceEvent({
    timestamp: toNano(Date.now()),
    fields: {},
    tags: {event, ...additionalTags},
  })
}

export const reportSimpleQueryPerformanceDuration = (
  event: string,
  startTime: number,
  duration: number
) => {
  reportQueryPerformanceEvent({
    timestamp: toNano(startTime),
    fields: {duration},
    tags: {event},
  })
}

export const useLoadTimeReporting = (event: string) => {
  const [loadStartTime] = useState(toNano(Date.now()))
  useEffect(() => {
    reportQueryPerformanceEvent({
      timestamp: loadStartTime,
      fields: {},
      tags: {event},
    })
  }, [])
}
