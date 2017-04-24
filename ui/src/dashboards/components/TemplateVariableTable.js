import React, {PropTypes} from 'react'

import TemplateVariableRow from 'src/dashboards/components/TemplateVariableRow'

const TemplateVariableTable = ({templates, onEditTemplateVariable}) => (
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
      {templates.map(t => (
        <TemplateVariableRow
          key={t.id}
          template={t}
          onEditTemplateVariable={onEditTemplateVariable}
        />
      ))}
    </div>
  </div>
)

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateVariableTable.propTypes = {
  templates: arrayOf(
    shape({
      type: string.isRequired,
      label: string.isRequired,
      tempVar: string.isRequired,
      query: shape({
        db: string.isRequired,
        influxql: string.isRequired,
      }),
      values: arrayOf(
        shape({
          value: string.isRequired,
          type: string.isRequired,
          selected: bool.isRequired,
        })
      ).isRequired,
    })
  ),
  onEditTemplateVariable: func.isRequired,
}

export default TemplateVariableTable
