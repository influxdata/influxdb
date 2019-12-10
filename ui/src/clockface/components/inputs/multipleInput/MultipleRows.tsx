// Libraries
import React, {SFC} from 'react'
import uuid from 'uuid'

// Components
import Row from 'src/clockface/components/inputs/multipleInput/Row'

interface Item {
  text?: string
  name?: string
}

interface RowsProps {
  tags: Item[]
  confirmText?: string
  onDeleteTag?: (item: Item) => void
  onChange?: (index: number, value: string) => void
}

const Rows: SFC<RowsProps> = ({tags, onDeleteTag, onChange}) => {
  return (
    <div className="input-tag-list">
      {tags.map(item => {
        return (
          <Row
            index={tags.indexOf(item)}
            key={uuid.v4()}
            item={item}
            onDelete={onDeleteTag}
            onChange={onChange}
          />
        )
      })}
    </div>
  )
}

export default Rows
