// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import TooltipDescription from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipDescription'
import TooltipArguments from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipArguments'
import TooltipExample from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipExample'
import TooltipLink from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipLink'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  func: FluxToolbarFunction
}

const FunctionTooltipContents: FunctionComponent<Props> = ({
  func: {desc, args, example, link, name},
}) => {
  return (
    <div className="flux-function-docs" data-testid={`flux-docs--${name}`}>
      <DapperScrollbars autoHide={false}>
        <div className="flux-toolbar--popover">
          <TooltipDescription description={desc} />
          <TooltipArguments argsList={args} />
          <TooltipExample example={example} />
          <TooltipLink link={link} />
        </div>
      </DapperScrollbars>
    </div>
  )
}

export default FunctionTooltipContents
