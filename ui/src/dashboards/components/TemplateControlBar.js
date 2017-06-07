import React, {PropTypes} from 'react'
import classnames from 'classnames'

import Dropdown from 'shared/components/Dropdown'

import omit from 'lodash/omit'

const TemplateControlBar = ({
  templates,
  onSelectTemplate,
  onOpenTemplateManager,
  isOpen,
}) =>
  <div className={classnames('template-control-bar', {show: isOpen})}>
    <div className="template-control--container">
      <div className="template-control--controls">
        {templates.length
          ? templates.map(({id, values, tempVar}) => {
              const items = values.map(value => ({...value, text: value.value}))
              const selectedItem = items.find(item => item.selected) || items[0]
              const selectedText = selectedItem && selectedItem.text

              // TODO: change Dropdown to a MultiSelectDropdown, `selected` to
              // the full array, and [item] to all `selected` values when we update
              // this component to support multiple values
              return (
                <div key={id} className="template-control--dropdown">
                  <Dropdown
                    items={items}
                    buttonSize="btn-xs"
                    menuClass="dropdown-astronaut"
                    useAutoComplete={true}
                    selected={selectedText || '(No values)'}
                    onChoose={item =>
                      onSelectTemplate(id, [item].map(x => omit(x, 'text')))}
                  />
                  <label className="template-control--label">
                    {tempVar}
                  </label>
                </div>
              )
            })
          : <div className="template-control--empty">
              This dashboard does not have any Template Variables
            </div>}
      </div>
      <button
        className="btn btn-primary btn-sm template-control--manage"
        onClick={onOpenTemplateManager}
      >
        <span className="icon cog-thick" />
        Manage
      </button>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateControlBar.propTypes = {
  templates: arrayOf(
    shape({
      id: string.isRequired,
      values: arrayOf(
        shape({
          value: string.isRequired,
        })
      ),
      tempVar: string.isRequired,
    })
  ).isRequired,
  onSelectTemplate: func.isRequired,
  onOpenTemplateManager: func.isRequired,
  isOpen: bool,
}

export default TemplateControlBar
