import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OnChangeArg} from 'src/types/flux'

interface Props {
  funcID: string
  argKey: string
  value: string
  type: string
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
  inputType?: string
}

interface State {
  height: string
}

@ErrorHandling
class FuncArgTextArea extends PureComponent<Props, State> {
  private ref: React.RefObject<HTMLTextAreaElement>

  constructor(props: Props) {
    super(props)
    this.ref = React.createRef()
    this.state = {
      height: '100px',
    }
  }

  public componentDidMount() {
    this.setState({height: this.height})
  }

  public render() {
    const {argKey, value, type} = this.props

    return (
      <div className="func-arg">
        <label className="func-arg--label" htmlFor={argKey}>
          {argKey}
        </label>
        <div className="func-arg--value">
          <textarea
            className="func-arg--textarea form-control input-sm"
            name={argKey}
            value={value}
            spellCheck={false}
            autoFocus={true}
            autoComplete="off"
            placeholder={type}
            ref={this.ref}
            onChange={this.handleChange}
            onKeyUp={this.handleKeyUp}
            style={this.textAreaStyle}
          />
        </div>
      </div>
    )
  }

  private get textAreaStyle() {
    const style = {
      height: this.state.height,
    }

    return style
  }

  private get height(): string {
    const ref = this.ref.current
    if (!ref) {
      return '200px'
    }

    const {scrollHeight} = ref

    return `${scrollHeight}px`
  }

  private handleKeyUp = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    const target = e.target as HTMLTextAreaElement
    const height = `${target.scrollHeight}px`
    this.setState({height})
  }

  private handleChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    const {funcID, argKey, bodyID, declarationID} = this.props

    this.props.onChangeArg({
      funcID,
      key: argKey,
      value: e.target.value,
      declarationID,
      bodyID,
    })
  }
}

export default FuncArgTextArea
