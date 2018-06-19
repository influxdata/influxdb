export const errorThrown = (error, altText, alertType) => ({
  type: 'ERROR_THROWN',
  error,
  altText,
  alertType,
})
