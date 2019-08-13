import React, {FC} from 'react'
import {Icon, IconFont} from '@influxdata/clockface'

interface Props {
  row: {sent: boolean}
}

const SentTableField: FC<Props> = ({row: {sent}}) => {
  const modifier = sent ? 'sent' : 'not-sent'

  return (
    <div className={`sent-table-field sent-table-field--${modifier}`}>
      {sent ? (
        <Icon glyph={IconFont.Checkmark} />
      ) : (
        <Icon glyph={IconFont.AlertTriangle} />
      )}
    </div>
  )
}

export default SentTableField
