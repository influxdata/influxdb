// Libraries
import React, {Component} from 'react'

// Components
import {Form} from '@influxdata/clockface'
import QueryBuilderBucketDropdown from 'src/timeMachine/components/QueryBuilderBucketDropdown'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

export default class QueryBuilderDataCard extends Component<{}> {
  public render() {
    return (
      <BuilderCard>
        <BuilderCard.Header title="From" />
        <BuilderCard.Body>
          <Form.Element label="Bucket">
            <QueryBuilderBucketDropdown />
          </Form.Element>
        </BuilderCard.Body>
      </BuilderCard>
    )
  }
}
