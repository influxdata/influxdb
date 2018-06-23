import React, {PureComponent} from 'react'
import uuid from 'uuid'

import {ErrorHandling} from 'src/shared/decorators/errors'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const LI_HEIGHT = 35
const LI_MARGIN_BOTTOM = 3
const RESULTS_TO_DISPLAY = 10

interface Props {
  items: string[]
  defaultValue: string
  onUpdateDefaultTemplateValue: (v: string) => void
}

@ErrorHandling
class TemplatePreviewList extends PureComponent<Props> {
  public render() {
    const {items} = this.props

    return (
      <ul
        className="temp-builder-results--list"
        style={{height: `${this.resultsListHeight}px`}}
      >
        <FancyScrollbar>
          {items.map(item => {
            return (
              <li
                key={uuid.v4()}
                style={{
                  height: `${LI_HEIGHT}px`,
                  marginBottom: `${LI_MARGIN_BOTTOM}px`,
                  zIndex: 9010,
                }}
                onClick={this.selectDefault(item)}
              >
                {item}
                {this.defaultIndicator(item)}
              </li>
            )
          })}
        </FancyScrollbar>
      </ul>
    )
  }

  private selectDefault = item => () => {
    const {onUpdateDefaultTemplateValue} = this.props
    onUpdateDefaultTemplateValue(item)
  }

  private defaultIndicator(item: string): JSX.Element {
    const {defaultValue} = this.props
    if (item === defaultValue) {
      return <div>{' ******'}</div>
    }
  }

  private get resultsListHeight() {
    const {items} = this.props
    const count = Math.min(items.length, RESULTS_TO_DISPLAY)

    return count * (LI_HEIGHT + LI_MARGIN_BOTTOM)
  }
}

export default TemplatePreviewList
