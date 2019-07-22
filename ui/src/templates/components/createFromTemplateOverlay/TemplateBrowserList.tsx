// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import {TemplateSummary} from 'src/types'
import TemplateBrowserListItem from 'src/templates/components/createFromTemplateOverlay/TemplateBrowserListItem'

interface Props {
  templates: TemplateSummary[]
  selectedTemplateSummary: TemplateSummary
  onSelectTemplate: (selectedTemplateSummary: TemplateSummary) => void
}

class TemplateBrowser extends PureComponent<Props> {
  public render() {
    const {selectedTemplateSummary, templates, onSelectTemplate} = this.props

    return (
      <DapperScrollbars
        className="import-template-overlay--templates"
        autoSize={false}
        noScrollX={true}
      >
        {templates.map(t => (
          <TemplateBrowserListItem
            key={t.id}
            template={t}
            label={t.meta.name}
            onClick={onSelectTemplate}
            testID={`template--${t.meta.name}`}
            selected={_.get(selectedTemplateSummary, 'id', '') === t.id}
          />
        ))}
      </DapperScrollbars>
    )
  }
}

export default TemplateBrowser
