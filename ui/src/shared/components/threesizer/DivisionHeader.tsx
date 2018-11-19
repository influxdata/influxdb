import React, {PureComponent} from 'react'

interface Props {
  onMinimize: () => void
  onMaximize: () => void
  buttons: JSX.Element[]
  name?: string
}

class DivisionHeader extends PureComponent<Props> {
  public render() {
    return (
      <div className="threesizer--header">
        {this.renderName}
        <div className="threesizer--header-controls">
          {this.props.buttons.map(b => b)}
        </div>
      </div>
    )
  }

  private get renderName(): JSX.Element {
    const {name} = this.props

    if (!name) {
      return
    }

    return <div className="threesizer--header-name">{name}</div>
  }
}

export default DivisionHeader
