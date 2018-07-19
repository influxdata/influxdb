import React, {PureComponent} from 'react'
import uuid from 'uuid'

import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import TemplatePreviewListItem from 'src/tempVars/components/TemplatePreviewListItem'
import {TEMPLATE_PREVIEW_LIST_DIMENSIONS as DIMENSIONS} from 'src/tempVars/constants'

import {TemplateValue} from 'src/types'

const {RESULTS_TO_DISPLAY, LI_HEIGHT, LI_MARGIN_BOTTOM, OFFSET} = DIMENSIONS

interface Props {
  items: TemplateValue[]
  onUpdateDefaultTemplateValue: (item: TemplateValue) => void
}

@ErrorHandling
class TemplatePreviewList extends PureComponent<Props> {
  public render() {
    const {items, onUpdateDefaultTemplateValue} = this.props

    return (
      <div className="temp-builder--results-list">
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={this.resultsListHeight}
        >
          {items.map(item => (
            <TemplatePreviewListItem
              key={uuid.v4()}
              onClick={onUpdateDefaultTemplateValue}
              item={item}
            />
          ))}
        </FancyScrollbar>
      </div>
    )
  }

  private get resultsListHeight() {
    const {items} = this.props
    const count = Math.min(items.length, RESULTS_TO_DISPLAY)
    const scrollOffset = count > RESULTS_TO_DISPLAY ? OFFSET : 0
    return count * (LI_HEIGHT + LI_MARGIN_BOTTOM) - scrollOffset
  }
}

export default TemplatePreviewList
