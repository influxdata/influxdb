// Libraries
import React, {PureComponent} from 'react'
import ReactMarkdown from 'react-markdown'

// Components
import {DapperScrollbars} from '@influxdata/clockface'

// Utils
import {humanizeNote} from 'src/dashboards/utils/notes'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  text: string
}

@ErrorHandling
class Markdown extends PureComponent<Props> {
  public render() {
    const {text} = this.props

    return (
      <DapperScrollbars className="markdown-cell" autoHide={true}>
        <div className="markdown-cell--contents">
          <ReactMarkdown
            source={humanizeNote(text)}
            className="markdown-format"
          />
        </div>
      </DapperScrollbars>
    )
  }
}

export default Markdown
