import React, {PureComponent} from 'react'

import KeysTemplateBuilder from 'src/tempVars/components/KeysTemplateBuilder'
import {proxy} from 'src/utils/queryUrlGenerator'
import parseShowTagKeys from 'src/shared/parsing/showTagKeys'

import {TemplateBuilderProps, TemplateValueType} from 'src/types'

export const fetchTagKeys = async (
  source,
  db,
  measurement
): Promise<string[]> => {
  const {data} = await proxy({
    source: source.links.proxy,
    db,
    query: `SHOW TAG KEYS ON "${db}" FROM "${measurement}"`,
  })

  const {tagKeys} = parseShowTagKeys(data)

  return tagKeys
}

class TagKeysTemplateBuilder extends PureComponent<TemplateBuilderProps> {
  public render() {
    const {template, source, onUpdateTemplate} = this.props

    return (
      <KeysTemplateBuilder
        queryPrefix={'SHOW TAG KEYS ON'}
        templateValueType={TemplateValueType.TagKey}
        fetchKeys={fetchTagKeys}
        template={template}
        source={source}
        onUpdateTemplate={onUpdateTemplate}
      />
    )
  }
}

export default TagKeysTemplateBuilder
