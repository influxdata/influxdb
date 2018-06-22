enum AlertType {
  'info',
}

export type ErrorThrownActionCreator = (
  error: Error,
  altText?: string,
  alertType?: AlertType
) => ErrorThrownAction

interface ErrorThrownAction {
  type: 'ERROR_THROWN'
  error: ErrorDescription
  altText?: string
  alertType?: AlertType
}
export const errorThrown = (
  error: ErrorDescription,
  altText?: string,
  alertType?: AlertType
): ErrorThrownAction => ({
  type: 'ERROR_THROWN',
  error,
  altText,
  alertType,
})

interface ErrorDescription {
  status: number
  auth: {
    links: {
      me: string
    }
  }
}
