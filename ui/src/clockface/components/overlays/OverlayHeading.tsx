import React, {PureComponent, ReactChildren} from 'react'

interface Props {
  children?: ReactChildren | JSX.Element | JSX.Element[]
  title: string
  onDismiss?: () => void
  testID?: string
}

class OverlayHeading extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {title, onDismiss, children, testID} = this.props

    return (
      <div className="overlay--heading">
        <div
          className="overlay--title"
          data-testid={testID || 'overlay--title'}
        >
          {title}
        </div>
        {onDismiss && (
          <button
            className="overlay--dismiss"
            onClick={onDismiss}
            type="button"
            data-testid="button--dismiss"
          />
        )}
        {children && children}
      </div>
    )
  }
}
export default OverlayHeading
