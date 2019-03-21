// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import CopyButton from 'src/shared/components/CopyButton'

// Styles
import 'src/shared/components/CodeSnippet.scss'

export interface PassedProps {
  copyText: string
}

interface DefaultProps {
  label?: string
}

type Props = PassedProps & DefaultProps

@ErrorHandling
class CodeSnippet extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
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
          <CopyButton textToCopy={copyText} contentName={'Script'} />
          <label className="code-snippet--label">{label}</label>
        </div>
      </div>
    )
  }
}

export default CodeSnippet
