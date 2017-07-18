import React, {Component, PropTypes} from 'react'

// needs to be React Component for click handler to work
class CodeData extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {onClickTemplate, template} = this.props

    return (
      <code
        className="rule-builder--message-template"
        data-tip={template.text}
        onClick={onClickTemplate}
      >
        {template.label}
      </code>
    )
  }
}

const {func, shape, string} = PropTypes

CodeData.propTypes = {
  onClickTemplate: func,
  template: shape({
    label: string,
    text: string,
  }),
}

export default CodeData
