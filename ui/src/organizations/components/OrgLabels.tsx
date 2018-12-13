// Libraries
import React, {PureComponent} from 'react'

interface Props {
  labels: any[]
}

interface State {
  searchTerm: string
}

export default class Members extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    return <div>List Labels here</div>
  }
}
