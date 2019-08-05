import React, {CSSProperties, FC} from 'react'

interface Props {
  style: CSSProperties
}

const FooterRow: FC<Props> = ({style}) => {
  return (
    <div style={style}>
      <div className="event-footer-row">No more data found.</div>
    </div>
  )
}

export default FooterRow
