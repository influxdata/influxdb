import {useState, useEffect} from 'react'

import {
  reportPoints as reportPointsAPI,
  Point,
  PointTags,
  PointFields,
} from 'src/cloud/apis/reporting'

let reportingTags = {}
let reportingPoints = []
let reportDecayTimeout = null
let reportMaxTimeout = null

const REPORT_DECAY = 500 // number of miliseconds to wait after last event before sending
const REPORT_MAX_WAIT = 5000 // max number of miliseconds to wait between sends
const REPORT_MAX_LENGTH = 300 // max number of events to queue before sending

export const toNano = (ms: number) => ms * 1000000

export const updateReportingContext = (key: string, value: string) => {
  reportingTags = {...reportingTags, [key]: value}
}

export const reportEvent = ({timestamp, measurement, fields, tags}: Point) => {
  reportingPoints.push({
    measurement,
    tags: {...reportingTags, ...tags},
    fields: {...fields, source: 'ui'},
    timestamp,
  })

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

export const reportSimpleQueryPerformanceEvent = (event: string) => {
  reportQueryPerformanceEvent({
    timestamp: toNano(Date.now()),
    fields: {},
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
