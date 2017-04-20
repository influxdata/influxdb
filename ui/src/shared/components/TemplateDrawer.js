import React, {PropTypes} from 'react'

const TemplateDrawer = ({templates}) => (
  <div style={{display: 'flex', alignItems: 'center', fontWeight: '700'}}>
    {templates.map(t => <div key={t.tempVar}> {t.tempVar} </div>)}
  </div>
)

const {arrayOf, shape, string} = PropTypes

TemplateDrawer.propTypes = {
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ),
}

export default TemplateDrawer
