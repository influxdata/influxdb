import React, {PureComponent} from 'react'

interface Field {
  internalName: string
  displayName: string
  visible: boolean
  order?: number
}

interface Props {
  internalName: string
  displayName: string
  visible: boolean
  onFieldUpdate?: (field: Field) => void
  opacity: number
}

class GraphOptionsCustomizableField extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)

    this.handleFieldRename = this.handleFieldRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }

  public render() {
    const {internalName, displayName, visible, opacity} = this.props

    return (
      <div className="customizable-field">
        <div
          style={{opacity}}
          className={
            visible
              ? 'customizable-field--label'
              : 'customizable-field--label__hidden'
          }
          onClick={this.handleToggleVisible}
          title={
            visible
              ? `Click to HIDE ${internalName}`
              : `Click to SHOW ${internalName}`
          }
        >
          <span className={'icon shuffle'} />
          <span className={visible ? 'icon eye-open' : 'icon eye-closed'} />
          {internalName}
        </div>
        <input
          className="customizable-field--input"
          type="text"
          id="internalName"
          value={displayName}
          onBlur={this.handleFieldRename}
          placeholder={`Rename ${internalName}`}
          disabled={!visible}
        />
      </div>
    )
  }

  private handleFieldRename(rename: string) {
    const {onFieldUpdate, internalName, visible} = this.props
    onFieldUpdate({internalName, displayName: rename, visible})
  }

  private handleToggleVisible() {
    const {onFieldUpdate, internalName, displayName, visible} = this.props
    onFieldUpdate({internalName, displayName, visible: !visible})
  }
}

export default GraphOptionsCustomizableField
