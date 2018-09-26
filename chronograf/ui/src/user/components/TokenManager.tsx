// Libraries
import React, {Component} from 'react'

interface Props {
  token: string
}

class TokenManager extends Component<Props> {
  public render() {
    return <div>{this.props.token}</div>
  }
}

export default TokenManager
