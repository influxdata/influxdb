import {ErrorInfo} from 'react'
import HoneyBadger from 'honeybadger-js'
import {CLOUD, GIT_SHA} from 'src/shared/constants'

if (CLOUD) {
  HoneyBadger.configure({
    apiKey: process.env.HONEYBADGER_KEY,
    revision: GIT_SHA,
    environment: process.env.HONEYBADGER_ENV,
  })
}

interface AdditionalOptions {
  component?: string
}

export const reportError = (
  error: Error,
  additionalOptions?: AdditionalOptions
): void => {
  if (CLOUD) {
    HoneyBadger.notify(error, additionalOptions)
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
