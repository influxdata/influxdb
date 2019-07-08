// Libraries
import React, {FunctionComponent} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import TooltipDescription from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipDescription'
import TooltipArguments from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipArguments'
import TooltipExample from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipExample'
import TooltipLink from 'src/timeMachine/components/fluxFunctionsToolbar/TooltipLink'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

const MAX_HEIGHT = 400

interface Props {
  func: FluxToolbarFunction
}

const FunctionTooltipContents: FunctionComponent<Props> = ({
  func: {desc, args, example, link},
}) => {
  return (
    <div className="box-tooltip--contents">
      <FancyScrollbar autoHeight={true} maxHeight={MAX_HEIGHT} autoHide={false}>
        <TooltipDescription description={desc} />
        <TooltipArguments argsList={args} />
        <TooltipExample example={example} />
        <TooltipLink link={link} />
      </FancyScrollbar>
    </div>
  )
}

export default FunctionTooltipContents
