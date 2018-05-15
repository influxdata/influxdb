import React, {PureComponent} from 'react'
import MenuTooltipButton from 'src/shared/components/MenuTooltipButton'

const noop = () => {}

class DivisionHeader extends PureComponent {
  public render() {
    return (
      <div className="threesizer--header">
        <div className="threesizer-context--buttons">
          <MenuTooltipButton
            icon="pencil"
            informParent={noop}
            menuOptions={this.options}
          />
        </div>
      </div>
    )
  }

  private get options() {
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
