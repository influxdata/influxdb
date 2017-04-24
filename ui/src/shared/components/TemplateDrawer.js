import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'

const style = {
  display: 'flex',
  alignItems: 'center',
  fontWeight: '700',
  padding: '15px',
}

const TemplateDrawer = ({templates, selected, handleMouseOverTempVar}) => (
  <div style={style}>
    {templates.map(t => (
      <div
        onMouseOver={() => {
          handleMouseOverTempVar(t)
        }}
        style={{
          background: t.tempVar === selected.tempVar ? 'red' : 'transparent',
          marginRight: '5px',
          cursor: 'pointer',
        }}
        key={t.tempVar}
      >
        {' '}{t.tempVar}{' '}
      </div>
    ))}
  </div>
)

const {arrayOf, func, shape, string} = PropTypes

TemplateDrawer.propTypes = {
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ),
  selected: shape({
    tempVar: string,
  }),
  handleMouseOverTempVar: func.isRequired,
}

export default OnClickOutside(TemplateDrawer)
