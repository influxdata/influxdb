import React, {PureComponent} from 'react'
import uuid from 'uuid'

import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import TemplatePreviewListItem from 'src/tempVars/components/TemplatePreviewListItem'

import {TemplateValue} from 'src/types'

const RESULTS_TO_DISPLAY = 10
const LI_HEIGHT = 28
const LI_MARGIN_BOTTOM = 2

interface Props {
  items: TemplateValue[]
  onUpdateDefaultTemplateValue: (item: TemplateValue) => void
}

@ErrorHandling
class TemplatePreviewList extends PureComponent<Props> {
  public render() {
    const {items, onUpdateDefaultTemplateValue} = this.props

    return (
      <div
        className="temp-builder--results-list"
        style={{height: `${this.resultsListHeight}px`}}
      >
        <FancyScrollbar autoHide={false}>
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

    return count * (LI_HEIGHT + LI_MARGIN_BOTTOM)
  }
}

export default TemplatePreviewList
