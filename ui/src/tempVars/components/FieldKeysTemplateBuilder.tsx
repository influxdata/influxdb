import React, {PureComponent} from 'react'
import _ from 'lodash'

import {TemplateBuilderProps, TemplateValueType} from 'src/types'
import KeysTemplateBuilder from 'src/tempVars/components/KeysTemplateBuilder'
import {proxy} from 'src/utils/queryUrlGenerator'
import parseShowFieldKeys from 'src/shared/parsing/showFieldKeys'

const fetchKeys = async (source, db, measurement): Promise<string[]> => {
  const {data} = await proxy({
    source: source.links.proxy,
    db,
    query: `SHOW FIELD KEYS ON "${db}" FROM "${measurement}"`,
  })

  const {fieldSets} = parseShowFieldKeys(data)
  const fieldKeys = _.get(Object.values(fieldSets), '0', [])

  return fieldKeys
}

class FieldKeysTemplateBuilder extends PureComponent<TemplateBuilderProps> {
  public render() {
    const {template, source, onUpdateTemplate} = this.props

    return (
      <KeysTemplateBuilder
        queryPrefix={'SHOW FIELD KEYS ON'}
        templateValueType={TemplateValueType.FieldKey}
        fetchKeys={fetchKeys}
        template={template}
        source={source}
        onUpdateTemplate={onUpdateTemplate}
      />
    )
  }
}

export default FieldKeysTemplateBuilder
