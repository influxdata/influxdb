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
  testID?: string
  values?: StringMap
  defaults?: StringMap
}

// NOTE: this is just a simplified form of the resig classic:
// https://johnresig.com/blog/javascript-micro-templating/
function transform(template, vars) {
  const output = new Function(
    'vars',
    'var output=' +
      JSON.stringify(template).replace(
        /<%=(.+?)%>/g,
        '"+(vars["$1".trim()])+"'
      ) +
      ';return output;'
  )
  return output(vars)
}

@ErrorHandling
class TemplatedCodeSnippet extends PureComponent<Props> {
  public transform() {
    const text = this.props.template
    const vars = Object.assign({}, this.props.defaults, this.props.values)

    return transform(text, vars)
  }

  render() {
    const {label, testID} = this.props
    const props = {
      label,
      testID,
      copyText: this.transform(),
    }

    return <CodeSnippet {...props} />
  }
}

export {transform}
export default TemplatedCodeSnippet
