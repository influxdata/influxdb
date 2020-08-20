// Libraries
import React, {FC, MouseEvent, useContext, useCallback} from 'react'

// Components
import {List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

type Props = {
  tags: any[]
}

const TagSelectors: FC<Props> = ({tags}) => {
  const {data, update} = useContext(PipeContext)
  const {searchTerm} = useContext(SchemaContext)
  const selectedTags = data?.tags

  const handleSublistMultiSelect = useCallback(
    (tagName: string, tagValue: string): void => {
      let tagValues = []
      if (!selectedTags[tagName]) {
        tagValues = [tagValue]
      } else if (
        selectedTags[tagName] &&
        selectedTags[tagName].includes(tagValue)
      ) {
        tagValues = selectedTags[tagName].filter(v => v !== tagValue)
      } else {
        tagValues = [...selectedTags[tagName], tagValue]
      }
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
    <>
      {tags.map(tag => {
        return (
          <React.Fragment key={JSON.stringify(tag)}>
            {Object.entries(tag).map(([tagName, tagValues]) => {
              const values = tagValues as any[]
              return (
                <React.Fragment key={tagName}>
                  {values
                    .filter(tagValue => tagValue.includes(searchTerm))
                    .map(tagValue => (
                      <List.Item
                        key={tagValue}
                        value={tagValue}
                        onClick={(
                          value: string,
                          event: MouseEvent<HTMLDivElement, MouseEvent>
                        ) => {
                          handleSubListItemClick(event, tagName, value)
                        }}
                        selected={selectedTags[tagName]?.includes(tagValue)}
                        title={tagValue}
                        gradient={Gradients.GundamPilot}
                        wrapText={true}
                      >
                        <List.Indicator type="dot" />
                        <div className="data-tag--equation">{`${tagName} = ${tagValue}`}</div>
                        <div className="data-measurement--name">
                          &nbsp;tag key&nbsp;
                        </div>
                        <div className="data-measurement--type">string</div>
                      </List.Item>
                    ))}
                </React.Fragment>
              )
            })}
          </React.Fragment>
        )
      })}
    </>
  )
}

export default TagSelectors
