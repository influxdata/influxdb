// Libraries
import React, {FC, useContext, useCallback, useState, MouseEvent} from 'react'

// Components
import {
  // TechnoSpinner,
  // ComponentSize,
  // RemoteDataState,
  InfluxColors,
  Input,
  List,
  ListItemRef,
  Gradients,
} from '@influxdata/clockface'
// import {BucketContext} from 'src/notebooks/context/buckets'
import {PipeContext} from 'src/notebooks/context/pipe'

type Props = {
  schema: any
}

const TagSelector: FC<Props> = ({tags}) => {
  // TODO(ariel): refactor so that the measurement and fields are separate
  const {data, update} = useContext(PipeContext)
  // const {buckets, loading} = useContext(BucketContext)
  const [searchTerm, setSearchTerm] = useState('')
  const selectedField = data.field
  const selectedMeasurement = data.measurement
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

  // TODO(ariel): add some context

  let body

  // if (loading === RemoteDataState.Loading) {
  //   body = (
  //     <div className="data-source--list__empty">
  //       <TechnoSpinner strokeWidth={ComponentSize.Small} diameterPixels={32} />
  //     </div>
  //   )
  // }

  // if (loading === RemoteDataState.Error) {
  //   body = (
  //     <div className="data-source--list__empty">
  //       <p>Could not fetch schema</p>
  //     </div>
  //   )
  // }
  if (
    /*loading === RemoteDataState.Done &&*/ selectedMeasurement &&
    selectedField
  ) {
    body = (
      <List
        className="data-source--list"
        backgroundColor={InfluxColors.Obsidian}
      >
        {Object.entries(schema[selectedMeasurement].tags)
          .filter(([tagName, tagValues]) => {
            const values = tagValues as any[]
            if (tagName.includes(searchTerm)) {
              return true
            }
            return values?.some(val => val.includes(searchTerm))
          })
          .map(([tagName, tagValues]) => {
            const values = tagValues as any[]
            return (
              <>
                <List.Item
                  key={tagName}
                  value={tagName}
                  onClick={() => {}} // NOTE: this is here for styling due to clockface assumptions
                  selected={!!(tagName in selectedTags)}
                  title={tagName}
                  gradient={Gradients.GundamPilot}
                  wrapText={true}
                >
                  <List.Indicator type="dot" />
                  {tagName}
                </List.Item>
                <div className="data-subset--list">
                  {values
                    .filter(val => {
                      if (tagName.includes(searchTerm)) {
                        return true
                      }
                      return val.includes(searchTerm)
                    })
                    .map(val => (
                      <List.Item
                        key={val}
                        value={val}
                        onClick={(event: MouseEvent) =>
                          handleSubListItemClick(event, tagName, val)
                        }
                        selected={selectedTags[tagName]?.includes(val)}
                        title={val}
                        gradient={Gradients.GundamPilot}
                        wrapText={true}
                      >
                        <List.Indicator type="checkbox" />
                        {val}
                      </List.Item>
                    ))}
                </div>
              </>
            )
          })}
      </List>
    )
  }

  return (
    <div className="data-source--block">
      <div className="data-source--block-title">Tags</div>
      <Input
        value={searchTerm}
        placeholder="Search for a tag"
        className="tag-selector--search"
        onChange={e => setSearchTerm(e.target.value)}
      />
      {body}
    </div>
  )
}

export default TagSelector
