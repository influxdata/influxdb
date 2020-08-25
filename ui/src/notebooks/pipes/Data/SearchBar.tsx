// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Input, IconFont, ComponentSize} from '@influxdata/clockface'

// Contexts
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

// Utils
import {event} from 'src/cloud/utils/reporting'

const SearchBar: FC = () => {
  const {searchTerm, setSearchTerm} = useContext(SchemaContext)

  const handleSetSearch = (text: string): void => {
    event('Searching in Flow Query Builder')
    setSearchTerm(text)
  }

  return (
    <Input
      icon={IconFont.Search}
      size={ComponentSize.Medium}
      value={searchTerm}
      placeholder="Filter data by Measurement, Field, or Tag ..."
      className="tag-selector--search"
      onChange={e => handleSetSearch(e.target.value)}
    />
  )
}

export default SearchBar
