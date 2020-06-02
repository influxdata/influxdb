// RUM - Real User Monitoring
// Uses the Navigation timing API to calculate the performance of our system

import {getFlags} from 'src/client'
import {reportError} from 'src/shared/utils/errors'

export interface Tags {
  [key: string]: string
}

export interface Fields {
  [key: string]: number | string
}

// See https://www.w3.org/TR/navigation-timing/timing-overview.png
export const buildFieldsFromTiming = function buildFieldsFromTiming(
  timing: PerformanceNavigationTiming
): Fields {
  // These are counted from the start of the request, hence no maths
  const ttfb = timing.responseStart
  const pageLoad = timing.loadEventStart
  const domInteractive = timing.domInteractive

  const redirect = timing.redirectEnd - timing.redirectStart

  // Networking
  const pageDownload = timing.responseEnd - timing.fetchStart
  const latency = timing.responseStart - timing.fetchStart
  const dns = timing.domainLookupEnd - timing.domainLookupStart

  // Server measurements
  const serverConnect = timing.connectEnd - timing.connectStart
  const serverResponse = timing.responseStart - timing.requestStart
  const totalServer = timing.responseEnd - timing.requestStart

  //  UI measurements
  const domProcessing = timing.domComplete - timing.responseEnd
  const domContentLoading = timing.domComplete - timing.domInteractive
  const windowLoadEvent = timing.loadEventEnd - timing.loadEventStart

  let tls = 0
  if (timing.secureConnectionStart > 0) {
    tls = timing.connectEnd - timing.secureConnectionStart
  }

  let worker = 0
  if (timing.workerStart > 0) {
    worker = timing.responseEnd - timing.workerStart
  }

  return {
    dns,
    domContentLoading,
    domInteractive,
    domProcessing,
    latency,
    pageDownload,
    pageLoad,
    redirect,
    serverConnect,
    serverResponse,
    tls,
    totalServer,
    ttfb,
    windowLoadEvent,
    worker,
  }
}

export const writeNavigationTimingMetrics = async function writeNavigationTimingMetrics() {
  let flags
  try {
    // see flags.yml. This should always be false in OSS contexts
    const resp = await getFlags({})
    if (resp.status >= 300) {
      return
    }
    flags = resp.data
  } catch (error) {
    reportError(error, {name: 'navigation timing metrics: failed to get flags'})
    return
  }

  if (flags.appMetrics) {
    const measurement = 'rum'
    const tags: Tags = {
      pathname:
        window.location.pathname.replace(/[a-z0-9]{16}\/?/gi, '') ||
        window.location.pathname,
    }
    const navigationTiming = performance.getEntriesByType(
      'navigation'
    )[0] as PerformanceNavigationTiming
    const fields = buildFieldsFromTiming(navigationTiming)

    const line = {measurement, tags, fields}

    const url = '/api/v2/app-metrics'
    fetch(url, {
      method: 'POST',
      body: JSON.stringify(line),
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    })
  }
}
