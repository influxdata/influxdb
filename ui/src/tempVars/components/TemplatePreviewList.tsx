import React, {PureComponent} from 'react'
import uuid from 'uuid'

import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import TemplatePreviewListItem from 'src/tempVars/components/TemplatePreviewListItem'

const LI_HEIGHT = 35
const LI_MARGIN_BOTTOM = 3
const RESULTS_TO_DISPLAY = 10

interface Props {
  items: string[]
  onChoose: (item: string) => void
}

@ErrorHandling
class TemplatePreviewList extends PureComponent<Props> {
  public render() {
    const {items, onChoose} = this.props

    return (
      <ul
        className="temp-builder-results--list"
        style={{height: `${this.resultsListHeight}px`}}
      >
        <FancyScrollbar>
          {items.map(item => {
            return (
              <TemplatePreviewListItem
                key={uuid.v4()}
                onClick={onChoose}
                style={{
                  height: `${LI_HEIGHT}px`,
                  marginBottom: `${LI_MARGIN_BOTTOM}px`,
                }}
                item={item}
              />
            )
          })}
        </FancyScrollbar>
      </ul>
    )
  }

  private get resultsListHeight() {
    const {items} = this.props
    const count = Math.min(items.length, RESULTS_TO_DISPLAY)

    return count * (LI_HEIGHT + LI_MARGIN_BOTTOM)
  }
}

export default TemplatePreviewList
