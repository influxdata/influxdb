// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  onClick: () => Promise<void>
}

interface State {
  status: RemoteDataState
}

class DeleteSourceButton extends PureComponent<Props, State> {
  public state: State = {
    status: RemoteDataState.NotStarted,
  }

  public render() {
    const {status} = this.state

    const buttonStatus =
      status === RemoteDataState.Loading
        ? ComponentStatus.Loading
        : ComponentStatus.Default

    return (
      <Button
        text="Delete"
        color={ComponentColor.Danger}
        size={ComponentSize.ExtraSmall}
        status={buttonStatus}
        onClick={this.handleClick}
      />
    )
  }

  private handleClick = async () => {
    this.setState({status: RemoteDataState.Loading})
    this.props.onClick()
  }
}

export default DeleteSourceButton
