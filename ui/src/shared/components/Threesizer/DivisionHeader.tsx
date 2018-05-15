import React, {PureComponent} from 'react'
import DivisionMenu, {
  MenuItem,
} from 'src/shared/components/Threesizer/DivisionMenu'

class DivisionHeader extends PureComponent {
  public render() {
    return (
      <div className="threesizer--header">
        <DivisionMenu menuItems={this.menuItems} />
      </div>
    )
  }

  private get menuItems(): MenuItem[] {
    return [
      {
        action: () => {},
        text: 'Maximize',
      },
      {
        action: () => {},
        text: 'Minimize',
      },
    ]
  }
}

export default DivisionHeader
