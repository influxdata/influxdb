/* 
tslint:disable no-console 
tslint:disable max-classes-per-file
*/

import React, {ComponentClass, Component} from 'react'

class DefaultError extends Component {
  public render() {
    return (
      <p className="error">
        A Chronograf error has occurred. Please report the issue&nbsp;
        <a href="https://github.com/influxdata/platform/chronograf/issues">
          here
        </a>
        .
      </p>
    )
  }
}

export function ErrorHandlingWith(
  Error: ComponentClass, // Must be a class based component and not an SFC
  alwaysDisplay = false
) {
  return <P, S, T extends {new (...args: any[]): Component<P, S>}>(
    constructor: T
  ) => {
    class Wrapped extends constructor {
      public static get displayName(): string {
        return constructor.name
      }

      private error: boolean = false

      public componentDidCatch(err, info) {
        console.error(err)
        console.warn(info)
        this.error = true
        this.forceUpdate()
      }

      public render() {
        if (this.error || alwaysDisplay) {
          return <Error />
        }

        return super.render()
      }
    }

    return Wrapped
  }
}

export const ErrorHandling = ErrorHandlingWith(DefaultError)
