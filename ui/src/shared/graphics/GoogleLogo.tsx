// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  className?: string
}

export const GoogleLogo: FC<Props> = ({className}) => (
  <svg
    className={classnames('google-logo', className)}
    x="0"
    y="0"
    viewBox="0 0 17.6 18"
  >
    <path
      d="M15 15.8h-3v-2.3c1-.6 1.6-1.6 1.8-2.7H9V7.4h8.5c.1.6.2 1.2.2 1.8-.1 2.7-1 5.1-2.7 6.6z"
      className="google-logo--blue"
    />
    <path
      d="M9 18c-3.5 0-6.6-2-8-5v-2.3h3c.7 2.1 2.7 3.7 5 3.7 1.2 0 2.2-.3 3-.9l2.9 2.3C13.5 17.2 11.4 18 9 18z"
      className="google-logo--green"
    />
    <path
      d="M4 7.3c-.2.5-.3 1.1-.3 1.7 0 .6.1 1.2.3 1.7L1 13c-.6-1.2-1-2.6-1-4s.3-2.8 1-4h3v2.3z"
      className="google-logo--yellow"
    />
    <path
      d="M12.4 4.9C11.5 4 10.3 3.6 9 3.6c-2.3 0-4.3 1.6-5 3.7L1 5c1.4-3 4.5-5 8-5 2.4 0 4.5.9 6 2.3l-2.6 2.6z"
      className="google-logo--red"
    />
  </svg>
)
