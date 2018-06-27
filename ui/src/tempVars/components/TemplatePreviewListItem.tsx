import React, {PureComponent} from 'react'
import classNames from 'classnames'

import {TemplateValue} from 'src/types'

interface Props {
  item: TemplateValue
  onClick: (item: TemplateValue) => void
  style: React.CSSProperties
}

class TemplatePreviewListItem extends PureComponent<Props> {
  public render() {
    const {item, style} = this.props

    return (
      <li
        onClick={this.handleClick}
        style={style}
        className={classNames('temp-builder-results--list-item', {
          active: this.isDefault,
        })}
      >
        {item.value}
        {this.renderIndicator()}
      </li>
    )
  }

  private get isDefault(): boolean {
    return this.props.item.selected
  }

  private renderIndicator(): JSX.Element {
    if (this.isDefault) {
      return <div>{' default'}</div>
    }
  }

  private handleClick = (): void => {
    if (this.isDefault) {
      return
    }

    this.props.onClick(this.props.item)
  }
}

export default TemplatePreviewListItem
