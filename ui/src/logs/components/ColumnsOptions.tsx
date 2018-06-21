import React, {Component} from 'react'
import uuid from 'uuid'

interface Props {
  columns: string[]
}

class ColumnsOptions extends Component<Props> {
  public render() {
    const {columns} = this.props

    return (
      <>
        <label className="form-label">Order Table Columns</label>
        {columns.map(c => <p key={uuid.v4()}>{c}</p>)}
      </>
    )
  }
}

export default ColumnsOptions
