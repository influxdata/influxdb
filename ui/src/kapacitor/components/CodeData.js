import React, {PropTypes} from 'react'

const CodeData = ({onClickTemplate, template}) =>
  <code
    className="rule-builder--message-template"
    data-tip={template.text}
    onClick={onClickTemplate}
  >
    {template.label}
  </code>

const {func, shape, string} = PropTypes

CodeData.propTypes = {
  onClickTemplate: func,
  template: shape({
    label: string,
    text: string,
  }),
}

export default CodeData
