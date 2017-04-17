import React, {PropTypes} from 'react'

const TemplateVariableRow = ({
  template: {type, label, code, query, values},
}) => (
  <div>
    <div>{type}</div>
    <div>{label}</div>
    <div>{code}</div>
    <div>{query && query.text}</div>
    <div>{values}</div>
  </div>
)

const {arrayOf, shape, string} = PropTypes

TemplateVariableRow.propTypes = {
  template: shape({
    type: string.isRequired,
    label: string.isRequired,
    code: string.isRequired,
    query: shape({
      db: string.isRequired,
      text: string.isRequired,
    }),
    values: arrayOf(string.isRequired),
  }),
}

export default TemplateVariableRow
