import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  defaultValue?: boolean
  refFunc: (r: any) => void
  disableTest?: () => void
  isFormEditing: boolean
}

interface State {
  editing: boolean
}

@ErrorHandling
class RedactedInput extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      editing: false,
    }
  }

  public componentWillReceiveProps(nextProps) {
    if (!nextProps.isFormEditing) {
      this.setState({editing: false})
    }
  }

  public render() {
    const {defaultValue, id, refFunc, disableTest} = this.props
    const {editing} = this.state

    if (defaultValue === true && !editing) {
      return (
        <div className="form-control-static redacted-input">
          <span className="alert-value-set">
            <span className="icon checkmark" /> Value set
          </span>
          <div className="btn btn-xs btn-link" onClick={this.handleClick}>
            Change
          </div>
          <input
            className="form-control"
            id={id}
            type="hidden"
            ref={refFunc}
            defaultValue={defaultValue.toString()}
          />
        </div>
      )
    }

    return (
      <input
        className="form-control"
        id={id}
        type="text"
        ref={refFunc}
        defaultValue={''}
        onChange={disableTest}
      />
    )
  }

  private handleClick = () => {
    this.setState({editing: true})
  }
}

export default RedactedInput
