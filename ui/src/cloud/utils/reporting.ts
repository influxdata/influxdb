import {useState, useEffect} from 'react'

import {
  reportPoints as reportPointsAPI,
  Point,
  PointTags,
  PointFields,
} from 'src/cloud/apis/reporting'

let reportingTags = {}
let reportingPoints = []
let isReportScheduled = false
const reportingInterval = 5 // seconds

const toNano = (ms: number) => ms * 1000000

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

  if (!isReportScheduled) {
    isReportScheduled = true
    setTimeout(() => {
      const tempPoints = reportingPoints
      reportingPoints = []
      isReportScheduled = false
      reportPointsAPI({
        points: tempPoints,
      })
    }, reportingInterval * 1000)
  }
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
