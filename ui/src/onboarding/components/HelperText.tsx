import React, {FC} from 'react'
import {
  FontWeight,
  Heading,
  HeadingElement,
  Typeface,
} from '@influxdata/clockface'

const HelperText: FC = ({children}) => (
  <Heading
    element={HeadingElement.H6}
    type={Typeface.Rubik}
    weight={FontWeight.Regular}
    className="helper-link-text"
    selectable={true}
  >
    {children}
  </Heading>
)

export default HelperText
