// Libraries
import React, {PureComponent} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'

// Utils
import {humanizeNote} from 'src/dashboards/utils/notes'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

interface Props {
  text: string
}

@ErrorHandling
class ScrollableMarkdown extends PureComponent<Props> {
  public render() {
    const {text} = this.props

    return (
      <DapperScrollbars className="markdown-cell" autoHide={true}>
        <div className="markdown-cell--contents">
          <MarkdownRenderer
            text={humanizeNote(text)}
            className="markdown-format"
          />
        </div>
      </DapperScrollbars>
    )
  }
}

export default ScrollableMarkdown
