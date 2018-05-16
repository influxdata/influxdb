import React, {PureComponent} from 'react'
import DivisionMenu, {
  MenuItem,
} from 'src/shared/components/threesizer/DivisionMenu'

interface Props {
  onMinimize: () => void
  onMaximize: () => void
  buttons: JSX.Element[]
  menuOptions?: MenuItem[]
}

class DivisionHeader extends PureComponent<Props> {
  public render() {
    return (
      <div className="threesizer--header">
        {this.props.buttons.map(b => b)}
        <DivisionMenu menuItems={this.menuItems} />
      </div>
    )
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
