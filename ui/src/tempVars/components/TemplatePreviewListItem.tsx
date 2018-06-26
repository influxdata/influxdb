import React, {PureComponent} from 'react'

import {TemplateValue} from 'src/types'

interface Props {
  item: TemplateValue
  onClick: (item: TemplateValue) => void
  style: {
    height: string
    marginBottom: string
  }
}

class TemplatePreviewListItem extends PureComponent<Props> {
  public render() {
    const {item, style} = this.props

    return (
      <li onClick={this.handleClick} style={style}>
        {item.value}
      </li>
    )
  }

  private handleClick = (): void => {
    this.props.onClick(this.props.item)
  }
}

export default TemplatePreviewListItem
