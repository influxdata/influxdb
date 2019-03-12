import React, {PureComponent, ReactChildren} from 'react'

interface Props {
  children?: ReactChildren | JSX.Element | JSX.Element[]
  title: string
  onDismiss?: () => void
}

class OverlayHeading extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {title, onDismiss, children} = this.props

    return (
      <div className="overlay--heading">
        <div className="overlay--title">{title}</div>
        {onDismiss && (
          <button
            className="overlay--dismiss"
            onClick={onDismiss}
            type="button"
          />
        )}
        {children && children}
      </div>
    )
  }
}
export default OverlayHeading
