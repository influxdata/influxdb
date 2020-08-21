// Libraries
import React, {FC, useContext} from 'react'

// Components
import {InfluxColors, Input, List} from '@influxdata/clockface'
import FilterTags from 'src/notebooks/pipes/Data/FilterTags'
import MeasurementSelectors from 'src/notebooks/pipes/Data/MeasurementSelectors'
import FieldSelectors from 'src/notebooks/pipes/Data/FieldSelectors'
import TagSelectors from 'src/notebooks/pipes/Data/TagSelectors'
import {SchemaContext} from 'src/notebooks/context/schemaProvider'
import {PipeContext} from 'src/notebooks/context/pipe'
import FluxMonacoEditor from 'src/shared/components/FluxMonacoEditor'
import CopyButton from 'src/shared/components/CopyButton'

// Types
import {TagValues} from 'src/types'

// Constants
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

const queryTextTranspiler = () => {
  const {data} = useContext(PipeContext)
  let text = `from(bucket: "${data?.bucketName || 'v.bucket'}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)`
  if (data?.measurement) {
    text += `\n  |> filter(fn: (r) => r["_measurement"] == "${data.measurement}")`
  }
  if (data?.field) {
    text += `\n  |> filter(fn: (r) => r["_field"] == "${data.field}")`
  }
  const tags = data?.tags
  if (tags && Object.keys(tags)?.length > 0) {
    Object.keys(tags)
      .filter((tagName: string) => !!tags[tagName])
      .forEach((tagName: string) => {
        const tagValues = tags[tagName]
        const values = tagValues as TagValues
        if (values.length === 1) {
          text += `\n  |> filter(fn: (r) => r["${tagName}"] == "${values[0]}")`
        } else {
          values.forEach((val, i) => {
            if (i === 0) {
              text += `\n  |> filter(fn: (r) => r["${tagName}"] == "${val}"`
            }
            if (values.length - 1 === i) {
              text += ` or r["${tagName}"] == "${val}")`
            } else {
              text += ` or r["${tagName}"] == "${val}"`
            }
          })
        }
      })
  }
  return text
}

const Selectors: FC = () => {
  const {fields, measurements, searchTerm, setSearchTerm, tags} = useContext(
    SchemaContext
  )
  // TODO(ariel) uncomment for demo
  // return (
  //   <div className="data-source--block-results">
  //     <div className="data-source--block-title">
  //       <FilterTags />
  //     </div>
  //     <Input
  //       value={searchTerm}
  //       placeholder="Type to filter by Measurement, Field, or Tag ..."
  //       className="tag-selector--search"
  //       onChange={e => setSearchTerm(e.target.value)}
  //     />
  //     <List
  //       className="data-source--list"
  //       backgroundColor={InfluxColors.Obsidian}
  //       maxHeight="300px"
  //       style={{height: '300px'}}
  //     >
  //       <MeasurementSelectors measurements={measurements} />
  //       <FieldSelectors fields={fields} />
  //       <TagSelectors tags={tags} />
  //     </List>
  //   </div>
  // )
  const handleCopyAttempt = (copiedText: string, isSuccessful: boolean) => {
    const text = copiedText.slice(0, 30).trimRight()
    const truncatedText = `${text}...`

    if (isSuccessful) {
      return copyToClipboardSuccess(truncatedText, 'Flux Script')
    } else {
      return copyToClipboardFailed(truncatedText, 'Flux Script')
    }
  }

  return (
    <div className="data-source--block">
      <div className="data-source--block-title">
        <FilterTags />
      </div>
      <Input
        value={searchTerm}
        placeholder="Type to filter by Measurement, Field, or Tag ..."
        className="tag-selector--search"
        onChange={e => setSearchTerm(e.target.value)}
      />
      <div className="data-source--block-results">
        <List
          className="data-source--list"
          backgroundColor={InfluxColors.Obsidian}
          maxHeight="300px"
          style={{height: '300px'}}
        >
          <MeasurementSelectors measurements={measurements} />
          <FieldSelectors fields={fields} />
          <TagSelectors tags={tags} />
        </List>
      </div>
      <div className="data-source--block-results">
        <FluxMonacoEditor
          script={queryTextTranspiler()}
          onChangeScript={() => {}} // This is intentional so that it remains uneditable
        />
        <CopyButton
          textToCopy={queryTextTranspiler()}
          onCopyText={handleCopyAttempt}
          contentName="Script"
        />
      </div>
    </div>
  )
}

export default Selectors
