import React, {PureComponent} from 'react'
import SlideToggle from 'src/reusable_ui/components/slide_toggle/SlideToggle'
import {ComponentColor} from 'src/reusable_ui/types'

import {OnChangeArg} from 'src/types/flux'

interface Props {
  argKey: string
  value: boolean
  funcID: string
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
}

class FuncArgBool extends PureComponent<Props> {
  public render() {
    return (
      <div className="func-arg">
        <label className="func-arg--label">{this.props.argKey}</label>
        <div className="func-arg--value">
          <SlideToggle
            active={this.props.value}
            onChange={this.handleToggleClick}
            color={ComponentColor.Success}
          />
        </div>
      </div>
    )
  }

  private handleToggleClick = (): void => {
    const {
      argKey,
      funcID,
      bodyID,
      onChangeArg,
      declarationID,
      value,
    } = this.props
    onChangeArg({
      key: argKey,
      value: !value,
      funcID,
      bodyID,
      declarationID,
      generate: true,
    })
  }
}

export default FuncArgBool
