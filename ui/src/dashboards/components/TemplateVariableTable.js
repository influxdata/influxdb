import React, {PropTypes} from 'react'

import TemplateVariableRow from 'src/dashboards/components/TemplateVariableRow'

const TemplateVariableTable = ({templates}) => (
  <table className="table v-center">
    <thead>
      <tr>
        <th>Dropdown Label</th>
        <th>Shortcode</th>
        <th>Type</th>
        <th>Queries</th>
        <th>Values</th>
        <th />
      </tr>
    </thead>
    <tbody>
      {templates.map(t => <TemplateVariableRow key={t.id} template={t} />)}
    </tbody>
  </table>
)

const {arrayOf, shape, string} = PropTypes

TemplateVariableTable.propTypes = {
  templates: arrayOf(
    shape({
      type: string.isRequired,
      label: string.isRequired,
      code: string.isRequired,
      query: shape({
        db: string.isRequired,
        text: string.isRequired,
      }),
      values: arrayOf(string.isRequired),
    })
  ),
}

export default TemplateVariableTable
