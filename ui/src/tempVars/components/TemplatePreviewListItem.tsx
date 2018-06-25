import React, {PureComponent} from 'react'

interface Props {
  item: string
  onClick: (item: string) => void
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
        {item}
      </li>
    )
  }

  private handleClick = (): void => {
    this.props.onClick(this.props.item)
  }
}

export default TemplatePreviewListItem
