// Libraries
import React, {Component} from 'react'

interface Props {
  blargh: string
}

class UserSettings extends Component<Props> {
  public render() {
    return <div>{this.props.blargh}</div>
  }
}

export default UserSettings
