// Libraries
import React, {Component} from 'react'

// Components
import {Form} from 'src/clockface'
import QueryBuilderBucketDropdown from 'src/timeMachine/components/QueryBuilderBucketDropdown'

export default class QueryBuilderDataCard extends Component<{}> {
  public render() {
    return (
      <div className="tag-selector data-card">
        <Form.Element label="Bucket">
          <QueryBuilderBucketDropdown />
        </Form.Element>
      </div>
    )
  }
}
