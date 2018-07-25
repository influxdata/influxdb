import React, {PureComponent} from 'react'
import classNames from 'classnames'
import {TEMPLATE_PREVIEW_LIST_DIMENSIONS as DIMENSIONS} from 'src/tempVars/constants'

import {TemplateValue, TemplateValueType} from 'src/types'

const {LI_HEIGHT, LI_MARGIN_BOTTOM} = DIMENSIONS

const ITEM_STYLE = {
  height: `${LI_HEIGHT}px`,
  marginBottom: `${LI_MARGIN_BOTTOM}px`,
}

interface Props {
  item: TemplateValue
  onClick: (item: TemplateValue) => void
}

class TemplatePreviewListItem extends PureComponent<Props> {
  public render() {
    const {item} = this.props

    return (
      <li
        onClick={this.handleClick}
        style={ITEM_STYLE}
        className={classNames('temp-builder--results-item', {
          active: this.isDefault,
        })}
      >
        {this.mapTempVarKey}
        {item.value}
        {this.defaultIndicator()}
      </li>
    )
  }

  private get isDefault(): boolean {
    return this.props.item.selected
  }

  private get mapTempVarKey(): string {
    const {item} = this.props
    if (item.type === TemplateValueType.Map) {
      return `${item.key} --> `
    }
  }

  private defaultIndicator(): JSX.Element {
    if (this.isDefault) {
      return <div className="temp-builder--default">DEFAULT</div>
    }

    return <div className="temp-builder--default">SET AS DEFAULT</div>
  }

  private handleClick = (): void => {
    if (this.isDefault) {
      return
    }

    this.props.onClick(this.props.item)
  }
}

export default TemplatePreviewListItem
