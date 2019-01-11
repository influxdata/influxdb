// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import ScraperRow from 'src/organizations/components/ScraperRow'

// DummyData
import {resouceOwner} from 'src/organizations/dummyData'

interface Props {
  emptyState: JSX.Element
}

export default class BucketList extends PureComponent<Props> {
  public render() {
    const dummyData = resouceOwner

    const {emptyState} = this.props
    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="50%" />
            <IndexList.HeaderCell columnName="Bucket" width="50%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {dummyData.map(scraper => (
              <ScraperRow scraper={scraper} />
            ))}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }
}
