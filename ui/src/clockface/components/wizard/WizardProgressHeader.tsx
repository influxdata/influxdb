// Libraries
import React, {SFC} from 'react'

interface Props {
  children: any
}

const WizardProgressHeader: SFC<Props> = (props: Props) => {
  const {children} = props
  return <div className="wizard--progress-header">{children}</div>
}

export default WizardProgressHeader
