import React, {PureComponent} from 'react'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import CodeSnippet from 'src/shared/components/CodeSnippet'

interface StringMap {
  [key: string]: string
}

export interface Props {
  template: string
  label: string
  values?: StringMap
  defaults?: StringMap
}

@ErrorHandling
class TemplatedCodeSnippet extends PureComponent<Props> {
  // NOTE: this is just a simplified form of the resig classic:
  // https://johnresig.com/blog/javascript-micro-templating/
  public transform() {
    const text = this.props.template
    const output = new Function(
      'vars',
      'var output=' +
        JSON.stringify(text).replace(/<%=(.+?)%>/g, '"+(vars["$1".trim()])+"') +
        ';return output;'
    )
    return output(Object.assign({}, this.props.defaults, this.props.values))
  }

  render() {
    const {label} = this.props
    const props = {
      label,
      copyText: this.transform(),
    }

    return <CodeSnippet {...props} />
  }
}

export default TemplatedCodeSnippet
