// Libraries
import React, {Component} from 'react'
import ReactMarkdown from 'react-markdown'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Utils
import {humanizeNote} from 'src/dashboards/utils/notes'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  text: string
}

@ErrorHandling
class Markdown extends Component<Props> {
  public render() {
    const {text} = this.props

    return (
      <FancyScrollbar className="markdown-cell" autoHide={true}>
        <div className="markdown-cell--contents">
          <ReactMarkdown
            source={humanizeNote(text)}
            className="markdown-format"
          />
        </div>
      </FancyScrollbar>
    )
  }
}

export default Markdown
