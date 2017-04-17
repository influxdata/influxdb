import React, {PropTypes} from 'react'

import TemplateVariableRow from 'src/dashboards/components/TemplateVariableRow'

const TemplateVariableTable = ({templates}) => (
  <div>
    {templates.map(t => <TemplateVariableRow key={t.id} template={t} />)}
  </div>
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
