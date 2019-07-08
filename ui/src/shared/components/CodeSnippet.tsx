// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import CopyButton from 'src/shared/components/CopyButton'

export interface Props {
  copyText: string
  label: string
}

@ErrorHandling
class CodeSnippet extends PureComponent<Props> {
  public static defaultProps = {
    label: 'Code Snippet',
  }

  public render() {
    const {copyText, label} = this.props
    return (
      <div className="code-snippet">
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={400}
          className="code-snippet--scroll"
        >
          <div className="code-snippet--text">
            <pre>
              <code>{copyText}</code>
            </pre>
          </div>
        </FancyScrollbar>
        <div className="code-snippet--footer">
          <CopyButton textToCopy={copyText} contentName="Script" />
          <label className="code-snippet--label">{label}</label>
        </div>
      </div>
    )
  }
}

export default CodeSnippet
