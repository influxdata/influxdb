// Libraries
import React, {Component} from 'react'

// Components
import DefaultErrorMessage from 'src/shared/components/DefaultErrorMessage'

// Types
import {ErrorMessageComponent} from 'src/types'

interface ErrorBoundaryProps {
  errorComponent: ErrorMessageComponent
}

interface ErrorBoundaryState {
  error: Error
}

class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  public static defaultProps = {errorComponent: DefaultErrorMessage}

  public state: ErrorBoundaryState = {error: null}

  public static getDerivedStateFromError(error: Error) {
    return {error}
  }

  public render() {
    const {error} = this.state

    if (error) {
      return <this.props.errorComponent error={error} />
    }

    return this.props.children
  }
}

export default ErrorBoundary
