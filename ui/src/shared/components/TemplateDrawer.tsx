import React, {SFC, MouseEvent} from 'react'
import OnClickOutside from 'react-onclickoutside'
import classnames from 'classnames'

import {Template} from 'src/types'

interface Props {
  templates: Template[]
  selected: Template
  onMouseOverTempVar: (
    template: Template
  ) => (e: MouseEvent<HTMLDivElement>) => void
  onClickTempVar: (
    template: Template
  ) => (e: MouseEvent<HTMLDivElement>) => void
}
const TemplateDrawer: SFC<Props> = ({
  templates,
  selected,
  onMouseOverTempVar,
  onClickTempVar,
}) => (
  <div className="template-drawer">
    {templates.map(t => (
      <div
        className={classnames('template-drawer--item', {
          'template-drawer--selected': t.tempVar === selected.tempVar,
        })}
        onMouseOver={onMouseOverTempVar(t)}
        onMouseDown={onClickTempVar(t)}
        key={t.tempVar}
      >
        {' '}
        {t.tempVar}{' '}
      </div>
    ))}
  </div>
)

export default OnClickOutside(TemplateDrawer)
