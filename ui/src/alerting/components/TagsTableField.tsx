// Libraries
import React, {FC} from 'react'

interface Props {
  row: {tags: {[tagKey: string]: string}}
}

const TagsTableField: FC<Props> = ({row: {tags}}) => {
  return (
    <div className="tags-table-field">
      {Object.keys(tags).map(key => (
        <div key={key} className="tags-table-field--tag">
          <span className="tags-table-field--key">{key}</span>=
          <span className="tags-table-field--value">{tags[key]}</span>
        </div>
      ))}
    </div>
  )
}

export default TagsTableField
