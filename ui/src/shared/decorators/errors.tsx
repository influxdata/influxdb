import React from 'react'

export function ErrorHandling<
  P,
  S,
  T extends {new (...args: any[]): React.Component<P, S>}
>(constructor: T) {
  class Wrapped extends constructor {
    private error: boolean = false

    public componentDidCatch(error, info) {
      console.error(error)
      console.error(info)
      this.error = true
      this.forceUpdate()
    }

    public render() {
      if (this.error) {
        return (
          <p className="error">
            A Chronograf error has occurred. Please report the issue
            <a href="https://github.com/influxdata/chronograf/issues">here</a>
          </p>
        )
      }

      return super.render()
    }
  }
  return Wrapped
}
