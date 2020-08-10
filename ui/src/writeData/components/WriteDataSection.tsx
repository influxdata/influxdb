// Libraries
import React, {FC, useContext} from 'react'
import {sortBy} from 'lodash'

// Contexts
import {WriteDataSearchContext} from 'src/writeData/containers/WriteDataPage'

// Components
import {
  SquareGrid,
  ComponentSize,
  Heading,
  HeadingElement,
  FontWeight,
} from '@influxdata/clockface'
import WriteDataItem from 'src/writeData/components/WriteDataItem'

// Constants
import {doesItemMatchSearchTerm} from 'src/writeData/constants'

// Types
import {WriteDataSection} from 'src/writeData/constants'

const WriteDataSection: FC<WriteDataSection> = ({
  id,
  name,
  description,
  items,
}) => {
  const {searchTerm} = useContext(WriteDataSearchContext)

  const filteredItems = items.filter(item =>
    doesItemMatchSearchTerm(item.name, searchTerm)
  )

  const sortedItems = sortBy(filteredItems, item => item.name)

  return (
    <div
      className="write-data--section"
      data-testid={`write-data--section ${id}`}
    >
      <Heading
        element={HeadingElement.H2}
        weight={FontWeight.Regular}
        style={{marginTop: '24px', marginBottom: '4px'}}
      >
        {name}
      </Heading>
      <Heading
        element={HeadingElement.H5}
        weight={FontWeight.Regular}
        style={{marginBottom: '12px'}}
      >
        {description}
      </Heading>
      <SquareGrid cardSize="170px" gutter={ComponentSize.Small}>
        {sortedItems.map(item => (
          <WriteDataItem
            key={item.id}
            id={item.id}
            name={item.name}
            image={item.image}
            url={item.url}
          />
        ))}
      </SquareGrid>
    </div>
  )
}

export default WriteDataSection
