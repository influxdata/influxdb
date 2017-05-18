import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import omit from 'lodash/omit'

const TemplateControlBar = ({
  templates,
  onSelectTemplate,
  onOpenTemplateManager,
}) => (
  <div className="template-control-bar">
    <div className="template-control--controls">
      {templates.map(({id, values, tempVar}) => {
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
              useAutoComplete={true}
              selected={selectedText || 'Loading...'}
              onChoose={item =>
                onSelectTemplate(id, [item].map(x => omit(x, 'text')))}
            />
            <label className="template-control--label">
              {tempVar}
            </label>
          </div>
        )
      })}
    </div>
    <button
      className="btn btn-primary btn-sm template-control--manage"
      onClick={onOpenTemplateManager}
    >
      <span className="icon cog-thick" />
      Templates
    </button>
  </div>
)

const {arrayOf, func, shape, string} = PropTypes

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
}

export default TemplateControlBar
