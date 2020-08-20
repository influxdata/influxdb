// Libraries
import React, {FC, useContext} from 'react'

// Components
import {InfluxColors, Input, List} from '@influxdata/clockface'
import FilterTags from 'src/notebooks/pipes/Data/FilterTags'
import MeasurementSelectors from 'src/notebooks/pipes/Data/MeasurementSelectors'
import FieldSelectors from 'src/notebooks/pipes/Data/FieldSelectors'
import TagSelectors from 'src/notebooks/pipes/Data/TagSelectors'
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

const Selectors: FC = () => {
  const {fields, measurements, searchTerm, setSearchTerm, tags} = useContext(
    SchemaContext
  )
  return (
    <div className="data-source--block-results">
      <div className="data-source--block-title">
        <FilterTags />
      </div>
      <Input
        value={searchTerm}
        placeholder="Type to filter by Measurement, Field, or Tag ..."
        className="tag-selector--search"
        onChange={e => setSearchTerm(e.target.value)}
      />
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
  )
}

export default Selectors
