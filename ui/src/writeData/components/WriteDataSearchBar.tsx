// Libraries
import React, {FC, ChangeEvent, useContext} from 'react'

// Contexts
import {WriteDataSearchContext} from 'src/writeData/containers/WriteDataPage'

// Components
import {Input, InputRef, ComponentSize, IconFont} from '@influxdata/clockface'

const WriteDataSearchBar: FC = () => {
  const {searchTerm, setSearchTerm} = useContext(WriteDataSearchContext)
  const handleInputChange = (e: ChangeEvent<InputRef>): void => {
    setSearchTerm(e.target.value)
  }

  return (
    <Input
      placeholder="Search data writing methods..."
      value={searchTerm}
      size={ComponentSize.Large}
      icon={IconFont.Search}
      onChange={handleInputChange}
      autoFocus={true}
    />
  )
}

export default WriteDataSearchBar
