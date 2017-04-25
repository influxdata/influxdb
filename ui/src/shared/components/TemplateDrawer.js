import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'

const style = {
  display: 'flex',
  alignItems: 'center',
  fontWeight: '700',
  padding: '15px',
}

const TemplateDrawer = ({
  templates,
  selected,
  onMouseOverTempVar,
  onClickTempVar,
}) => (
  <div style={style}>
    {templates.map(t => (
      <div
        onMouseOver={() => {
          onMouseOverTempVar(t)
        }}
        onClick={() => onClickTempVar(t)}
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
  onMouseOverTempVar: func.isRequired,
  onClickTempVar: func.isRequired,
}

export default OnClickOutside(TemplateDrawer)
