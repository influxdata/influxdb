// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {RemoteDataState} from 'src/types'

// Styles
import 'src/clockface/components/spinners/SpinnerContainer.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
  className?: string
  loading: RemoteDataState
  spinnerComponent: JSX.Element
}

@ErrorHandling
class SpinnerContainer extends Component<Props> {
  public render() {
    const {loading, children, spinnerComponent} = this.props

    if (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    ) {
      return <div className={this.className}>{spinnerComponent}</div>
    }

    return children
  }

  private get className(): string {
    const {className} = this.props

    return classnames('spinner-container', {[`${className}`]: className})
  }
}

export default SpinnerContainer
