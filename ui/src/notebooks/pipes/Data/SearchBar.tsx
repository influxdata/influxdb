// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Input, IconFont, ComponentSize} from '@influxdata/clockface'

// Contexts
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

const SearchBar: FC = () => {
  const {searchTerm, setSearchTerm} = useContext(SchemaContext)
  return (
    <Input
      icon={IconFont.Search}
      size={ComponentSize.Medium}
      value={searchTerm}
      placeholder="Filter data by Measurement, Field, or Tag ..."
      className="tag-selector--search"
      onChange={e => setSearchTerm(e.target.value)}
    />
  )
}

export default SearchBar
