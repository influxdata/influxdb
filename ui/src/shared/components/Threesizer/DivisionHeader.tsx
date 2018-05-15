import React, {PureComponent} from 'react'
import DivisionMenu, {
  MenuItem,
} from 'src/shared/components/Threesizer/DivisionMenu'

interface Props {
  onMinimize: () => void
  onMaximize: () => void
}

class DivisionHeader extends PureComponent<Props> {
  public render() {
    return (
      <div className="threesizer--header">
        <DivisionMenu menuItems={this.menuItems} />
      </div>
    )
  }

  private get menuItems(): MenuItem[] {
    const {onMaximize, onMinimize} = this.props
    return [
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
