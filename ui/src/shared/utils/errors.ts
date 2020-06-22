import {ErrorInfo} from 'react'
import HoneyBadger from 'honeybadger-js'
import {CLOUD, GIT_SHA} from 'src/shared/constants'

import {getUserFlags} from 'src/shared/utils/featureFlag'
import {reportEvent, PointTags, PointFields} from 'src/cloud/utils/reporting'

if (CLOUD) {
  HoneyBadger.configure({
    apiKey: process.env.HONEYBADGER_KEY,
    revision: GIT_SHA,
    environment: process.env.HONEYBADGER_ENV,
  })
}

export const reportSingleErrorMetric = (
  tags: PointTags = {},
  fields: PointFields = {},
  measurement = 'ui_error'
) => {
  if (CLOUD) {
    const point = {measurement, tags, fields: {errorCount: 1, ...fields}}
    reportEvent(point)
  }
}

// See https://docs.honeybadger.io/lib/javascript/guides/reporting-errors.html#additional-options
interface HoneyBadgerAdditionalOptions {
  component?: string
  context?: {[key: string]: any}
  cookies?: {[key: string]: any}
  name?: string
  params?: {[key: string]: any}
}

export const reportError = (
  error: Error,
  additionalOptions?: HoneyBadgerAdditionalOptions
): void => {
  let additionalContext = {}
  if (additionalOptions && additionalOptions.context) {
    additionalContext = {...additionalOptions.context}
  }

  const context = {
    ...additionalContext,
    ...getUserFlags(),
  }

  let options: HoneyBadgerAdditionalOptions = {}
  if (additionalOptions) {
    options = {...additionalOptions}

    delete options.context // already included in the above context object
  }

  if (CLOUD) {
    HoneyBadger.notify(error, {context, ...options})

    let errorType = 'generic (untagged) error'
    if (options.name) {
      errorType = options.name
    } else if (options.component) {
      errorType = options.component
    }

    reportSingleErrorMetric({errorType})
  } else {
    const honeyBadgerContext = (HoneyBadger as any).context
    /* eslint-disable no-console */
    console.log('Context that would have been sent to HoneyBadger:')
    console.table({...honeyBadgerContext, ...context, ...options})
    /* eslint-enable no-console */
  }
}

/*
  Parse React's error boundary info message to provide the name of the
  component an error occured in.

  For example, given the following info message:

      The above error occurred in the <MePage> component:
          in MePage (created by ErrorBoundary(MePage))
          in ErrorBoundary (created by ErrorBoundary(MePage))
          in ErrorBoundary(MePage) (created by Connect(ErrorBoundary(MePage)))
          in Connect(ErrorBoundary(MePage)) (created by RouterContext)
          in SpinnerContainer (created by SetOrg)
          ...

  We will extract "MePage" as the component name.
*/
export const parseComponentName = (errorInfo: ErrorInfo): string => {
  const componentName = errorInfo.componentStack
    .trim()
    .split('\n')
    .map(s => s.split(' ')[1])[0]

  return componentName
}
