import React, {PureComponent} from 'react'
import uuid from 'uuid'

import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const LI_HEIGHT = 28
const LI_MARGIN_BOTTOM = 2
const RESULTS_TO_DISPLAY = 10

interface Props {
  items: string[]
}

@ErrorHandling
class TemplatePreviewList extends PureComponent<Props> {
  public render() {
    const {items} = this.props

    return (
      <ul
        className="temp-builder--results-list"
        style={{height: `${this.resultsListHeight}px`}}
      >
        <FancyScrollbar autoHide={false}>
          {items.map(db => {
            return (
              <li
                className="temp-builder--results-item"
                key={uuid.v4()}
                style={{
                  height: `${LI_HEIGHT}px`,
                  marginBottom: `${LI_MARGIN_BOTTOM}px`,
                }}
              >
                {db}
              </li>
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
