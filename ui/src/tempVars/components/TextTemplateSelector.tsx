import React, {
  PureComponent,
  ChangeEvent,
  FocusEvent,
  KeyboardEvent,
} from 'react'

import {getDeep} from 'src/utils/wrappers'

import {Template, TemplateValueType} from 'src/types'

interface Props {
  template: Template
  onUpdateTemplate: (template: Template) => Promise<void>
}

interface State {
  text: string
}

class TextTemplateSelector extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(props) {
    const text = getDeep<string>(props, 'template.values.0.value', '')

    return {text}
  }

  constructor(props) {
    super(props)

    this.state = {text: ''}
  }

  public render() {
    const {text} = this.state

    return (
      <input
        type="text"
        className="text-template-selector"
        value={text}
        onChange={this.handleChange}
        onKeyUp={this.handleKeyUp}
        onFocus={this.handleFocus}
        onBlur={this.submit}
        spellCheck={false}
      />
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({text: e.target.value})
  }

  private handleKeyUp = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      e.currentTarget.blur()
    }
  }

  private handleFocus = (e: FocusEvent<HTMLInputElement>): void => {
    const input = e.currentTarget

    input.setSelectionRange(0, input.value.length)
  }

  private submit = (): void => {
    const {template, onUpdateTemplate} = this.props
    const {text} = this.state

    onUpdateTemplate({
      ...template,
      values: [
        {
          value: text,
          type: TemplateValueType.Constant,
          selected: true,
          localSelected: true,
        },
      ],
    })
  }
}

export default TextTemplateSelector
