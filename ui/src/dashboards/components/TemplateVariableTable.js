import React, {PropTypes} from 'react'

import TemplateVariableRow from 'src/dashboards/components/TemplateVariableRow'

const TemplateVariableTable = ({templates}) => (
  <div className="table-custom">
    <div className="thead">
      <div className="tr">
        <div className="th">Dropdown Label</div>
        <div className="th">Shortcode</div>
        <div className="th">Type</div>
        <div className="th">Queries</div>
        <div className="th">Values</div>
        <div className="th" />
      </div>
    </div>
    <div className="tbody">
      {templates.map(t => <TemplateVariableRow key={t.id} template={t} />)}
    </div>
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
