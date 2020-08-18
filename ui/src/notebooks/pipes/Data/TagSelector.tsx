// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

type Props = {
  tagName: string
  tagValue: string
}

const TagSelector: FC<Props> = ({tagName, tagValue}) => {
  const {data, update} = useContext(PipeContext)
  const selectedTags = data?.tags

  const handleSublistMultiSelect = useCallback(
    (tagName: string, tagValue: string): void => {
      const tagValues = [...selectedTags[tagName], tagValue]
      // do a multi select deselect
      update({
        tags: {
          ...selectedTags,
          [tagName]: tagValues,
        },
      })
    },
    [update]
  )

  const handleSubListItemClick = useCallback(
    (event: MouseEvent, tagName: string, tagValue: string) => {
      if (event.metaKey) {
        handleSublistMultiSelect(tagName, tagValue)
        return
      }
      let updatedValue = [tagValue]
      let tags = {
        [tagName]: updatedValue,
      }
      if (selectedTags[tagName]?.includes(tagValue)) {
        updatedValue = []
        tags[tagName] = updatedValue
      }
      if (tagName in selectedTags && updatedValue.length === 0) {
        tags = {}
      }
      update({
        tags,
      })
    },
    [update]
  )

  return (
    <List.Item
      key={tagValue}
      value={tagValue}
      onClick={(event: MouseEvent) =>
        handleSubListItemClick(event, tagName, tagValue)
      }
      selected={selectedTags[tagName]?.includes(tagValue)}
      title={tagValue}
      gradient={Gradients.GundamPilot}
      wrapText={true}
    >
      <List.Indicator type="dot" />
      <div className="data-tag--equation">{`${tagName} = ${tagValue}`}</div>
      <div className="data-measurement--name">&nbsp;tag key&nbsp;</div>
      <div className="data-measurement--type">string</div>
    </List.Item>
  )
}

export default TagSelector
