import {useState, useEffect} from 'react'

import {isFlagEnabled} from 'src/shared/utils/featureFlag'

import {
  reportPoints as reportPointsAPI,
  Point,
  PointTags,
  PointFields,
} from 'src/cloud/apis/reporting'

let reportingTags = {}
let reportingPoints = []

export const updateReportingContext = (key: string, value: string) => {
  reportingTags = {...reportingTags, [key]: value}
}

export const throttledReport = () => {
  const tempPoints = reportingPoints
  reportingPoints = []
  reportPointsAPI({
    points: tempPoints,
  })
}
let isReportScheduled = false

export const reportEvent = ({timestamp, measurement, fields, tags}: Point) => {
  if (!isFlagEnabled('appMetrics')) {
    return
  }

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
    }, 5000)
  }
}

export const reportQPEvent = ({
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

export const reportSimpleQPEvent = (event: string, timestamp: number) => {
  reportQPEvent({
    timestamp,
    fields: {},
    tags: {event},
  })
}

export const useLoadTimeReporting = (event: string) => {
  const [loadStartTime] = useState(Date.now())
  useEffect(() => {
    reportQPEvent({
      timestamp: loadStartTime,
      fields: {},
      tags: {event},
    })
  }, [])
}
