// Libraries
import React, {SFC} from 'react'
import uuid from 'uuid'

// Components
import Row from 'src/onboarding/components/configureStep/streaming/Row'

// Types
import {TelegrafPluginName} from 'src/types/v2/dataLoaders'

// Actions
import {setConfigArrayValue} from 'src/onboarding/actions/dataLoaders'

interface Item {
  text?: string
  name?: string
}

interface RowsProps {
  tags: Item[]
  confirmText?: string
  onDeleteTag?: (item: Item) => void
  onSetConfigArrayValue: typeof setConfigArrayValue
  fieldName: string
  telegrafPluginName: TelegrafPluginName
}

const Rows: SFC<RowsProps> = ({
  tags,
  onDeleteTag,
  onSetConfigArrayValue,
  fieldName,
  telegrafPluginName,
}) => {
  return (
    <div className="input-tag-list">
      {tags.map(item => {
        return (
          <Row
            index={tags.indexOf(item)}
            key={uuid.v4()}
            item={item}
            onDelete={onDeleteTag}
            onSetConfigArrayValue={onSetConfigArrayValue}
            fieldName={fieldName}
            telegrafPluginName={telegrafPluginName}
          />
        )
      })}
    </div>
  )
}

export default Rows
