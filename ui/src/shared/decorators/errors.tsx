/* 
tslint:disable no-console 
tslint:disable max-classes-per-file
*/

// Libraries
import React, {Component, ErrorInfo} from 'react'

// Components
import DefaultErrorMessage from 'src/shared/components/DefaultErrorMessage'

// Utils
import {reportError, parseComponentName} from 'src/shared/utils/errors'

// Types
import {ErrorMessageComponent} from 'src/types'

export function ErrorHandlingWith(Error: ErrorMessageComponent) {
  return <P, S, T extends {new (...args: any[]): Component<P, S>}>(
    constructor: T
  ) => {
    class Wrapped extends constructor {
      public static get displayName(): string {
        return constructor.name
      }

      private error: Error = null

      public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
        this.error = error
        this.forceUpdate()

        reportError(error, {component: parseComponentName(errorInfo)})
      }

      public render() {
        if (this.error) {
          return <Error error={this.error} />
        }

        return super.render()
      }
    }

    return Wrapped
  }
}

export const ErrorHandling = ErrorHandlingWith(DefaultErrorMessage)
