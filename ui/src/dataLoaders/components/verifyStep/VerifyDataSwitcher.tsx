// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {RemoteDataState} from 'src/types'
import StatusIndicator from 'src/dataLoaders/components/verifyStep/lineProtocol/StatusIndicator'

interface Props {
  type: DataLoaderType
  onDecrementCurrentStep: () => void
  lpStatus: RemoteDataState
}

@ErrorHandling
export class VerifyDataSwitcher extends PureComponent<Props> {
  public render() {
    const {type, lpStatus} = this.props

    switch (type) {
      case DataLoaderType.LineProtocol:
        return (
          <StatusIndicator
            status={lpStatus}
            onClickRetry={this.handleClickRetry}
          />
        )
      default:
        return <div />
    }
  }

  private handleClickRetry = () => {
    this.props.onDecrementCurrentStep()
  }
}

export default VerifyDataSwitcher
