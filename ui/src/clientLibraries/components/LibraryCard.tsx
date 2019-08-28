// Libraries
import React, {PureComponent} from 'react'

// Components
import {ResourceCard} from '@influxdata/clockface'

// Mocks
import {ClientLibrary} from 'src/clientLibraries/constants/mocks'

interface OwnProps {
  library: ClientLibrary
}

type Props = OwnProps

class LibraryCard extends PureComponent<Props> {
  public render() {
    const {library} = this.props

    return (
      <ResourceCard
        key={`library-id--${library.id}`}
        testID="resource-card"
        name={
          <ResourceCard.Name name={library.name} testID="library-card--name" />
        }
        description={
          <ResourceCard.Description description={library.description} />
        }
      />
    )
  }
}

export default LibraryCard
