import React, {PureComponent} from 'react'
import DivisionMenu, {
  MenuItem,
} from 'src/shared/components/threesizer/DivisionMenu'

interface Props {
  onMinimize: () => void
  onMaximize: () => void
  buttons: JSX.Element[]
  menuOptions?: MenuItem[]
  name?: string
}

class DivisionHeader extends PureComponent<Props> {
  public render() {
    return (
      <div className="threesizer--header">
        {this.renderName}
        <div className="threesizer--header-controls">
          {this.props.buttons.map(b => b)}
          <DivisionMenu menuItems={this.menuItems} />
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

  private get menuItems(): MenuItem[] {
    const {onMaximize, onMinimize, menuOptions} = this.props
    return [
      ...menuOptions,
      {
        action: onMaximize,
        text: 'Maximize',
      },
      {
        action: onMinimize,
        text: 'Minimize',
      },
    ]
  }
}

export default DivisionHeader
